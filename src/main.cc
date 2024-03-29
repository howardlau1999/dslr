#include "dslr.h"
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/write.hpp>
#include <boost/log/trivial.hpp>
#include <cstdio>
#include <iostream>
#include <thread>

#include <rdmapp/executor.h>
#include <rdmapp/rdmapp.h>

using boost::asio::awaitable;
using boost::asio::co_spawn;
using boost::asio::detached;
using boost::asio::use_awaitable;
using boost::asio::ip::tcp;
namespace this_coro = boost::asio::this_coro;

#if defined(BOOST_ASIO_ENABLE_HANDLER_TRACKING)
#define use_awaitable                                                          \
  boost::asio::use_awaitable_t(__FILE__, __LINE__, __PRETTY_FUNCTION__)
#endif

rdmapp::device *device_ptr;
rdmapp::pd *pd_ptr;
rdmapp::cq *send_cq_ptr;
rdmapp::cq *recv_cq_ptr;

uint64_t lock_state = 0;
rdmapp::local_mr *server_lock_state_mr;

rdmapp::task<void> rdma_server_func(rdmapp::qp *qp_ptr) {
  auto local_mr_serialized = server_lock_state_mr->serialize();
  auto buffer_mr =
      pd_ptr->reg_mr(local_mr_serialized.data(), local_mr_serialized.size());
  co_await qp_ptr->send(&buffer_mr);
  BOOST_LOG_TRIVIAL(info) << "Sent mr addr=" << server_lock_state_mr->addr()
                          << " length=" << server_lock_state_mr->length()
                          << " rkey=" << server_lock_state_mr->rkey()
                          << " to client";
  std::vector<uint8_t> local_mr_buffer(1024);
  auto local_mr =
      pd_ptr->reg_mr(local_mr_buffer.data(), local_mr_buffer.size());
  auto [n, imm] = co_await qp_ptr->recv(&local_mr);
  BOOST_LOG_TRIVIAL(info) << "Received " << n << " bytes"
                          << " from client";
  delete qp_ptr;
}

rdmapp::task<void> rdma_client_func(rdmapp::qp *qp_ptr) {
  char remote_mr_serialized[rdmapp::remote_mr::kSerializedSize];
  auto buffer_mr =
      pd_ptr->reg_mr(remote_mr_serialized, sizeof(remote_mr_serialized));
  co_await qp_ptr->recv(&buffer_mr);
  auto remote_mr = rdmapp::remote_mr::deserialize(remote_mr_serialized);
  BOOST_LOG_TRIVIAL(info) << "Received mr addr=" << remote_mr.addr()
                          << " length=" << remote_mr.length()
                          << " rkey=" << remote_mr.rkey() << " from server";
  uint64_t curr_state = 0;
  uint64_t prev_state = 0;
  auto local_curr_mr =
      new rdmapp::local_mr(pd_ptr->reg_mr(&curr_state, sizeof(curr_state)));
  auto local_prev_mr =
      new rdmapp::local_mr(pd_ptr->reg_mr(&prev_state, sizeof(prev_state)));
  dslr::shared_mutex mutex(remote_mr, local_prev_mr, local_curr_mr, qp_ptr);
  BOOST_LOG_TRIVIAL(info) << "Initialized DSLR";
  co_await mutex.lock_shared(1);
  BOOST_LOG_TRIVIAL(info) << "Locked shared";
  co_await mutex.unlock_shared(1, std::chrono::microseconds(0));
  BOOST_LOG_TRIVIAL(info) << "Unlocked shared";
}

awaitable<void> send_qp(rdmapp::qp const &qp, tcp::socket &socket) {
  auto local_qp_data = qp.serialize();
  assert(local_qp_data.size() != 0);
  size_t local_qp_sent = 0;
  while (local_qp_sent < local_qp_data.size()) {
    local_qp_sent += co_await socket.async_write_some(
        boost::asio::buffer(local_qp_data.data() + local_qp_sent,
                            local_qp_data.size() - local_qp_sent),
        use_awaitable);
  }
}

awaitable<rdmapp::deserialized_qp> recv_qp(tcp::socket &socket) {
  size_t header_read = 0;
  uint8_t header[rdmapp::deserialized_qp::qp_header::kSerializedSize];
  while (header_read < rdmapp::deserialized_qp::qp_header::kSerializedSize) {
    header_read += co_await socket.async_read_some(
        boost::asio::buffer(
            header + header_read,
            rdmapp::deserialized_qp::qp_header::kSerializedSize - header_read),
        use_awaitable);
  }
  auto remote_qp = rdmapp::deserialized_qp::deserialize(header);
  BOOST_LOG_TRIVIAL(info) << "Received qp lid=" << remote_qp.header.lid
                          << " qp_num=" << remote_qp.header.qp_num
                          << " sq_psn=" << remote_qp.header.sq_psn
                          << " user_data_size="
                          << remote_qp.header.user_data_size;
  remote_qp.user_data.resize(remote_qp.header.user_data_size);
  if (remote_qp.header.user_data_size != 0) {
    size_t user_data_read = 0;
    while (user_data_read < remote_qp.header.user_data_size) {
      user_data_read += co_await socket.async_read_some(
          boost::asio::buffer(remote_qp.user_data.data() + user_data_read,
                              remote_qp.header.user_data_size - user_data_read),
          use_awaitable);
    }
  }
  co_return remote_qp;
}

awaitable<void> qp_acceptor(tcp::socket socket) {
  auto remote_qp = co_await recv_qp(socket);
  auto qp_ptr =
      new rdmapp::qp(remote_qp.header.lid, remote_qp.header.qp_num,
                     remote_qp.header.sq_psn, pd_ptr, send_cq_ptr, recv_cq_ptr);
  co_await send_qp(*qp_ptr, socket);
  rdma_server_func(qp_ptr).detach();
}

awaitable<void> connector(std::string const &host, std::string const &port,
                          rdmapp::qp *qp_ptr) {
  auto executor = co_await this_coro::executor;
  tcp::resolver::query query(host, port);
  tcp::resolver resolver(executor);
  auto endpoints = co_await resolver.async_resolve(query, use_awaitable);
  if (endpoints.empty()) {
    BOOST_LOG_TRIVIAL(error) << "No endpoints found";
    co_return;
  }
  tcp::socket socket(executor);
  co_await socket.async_connect(*endpoints.begin(), use_awaitable);
  BOOST_LOG_TRIVIAL(info) << "Connected to " << host << ":" << port;
  co_await send_qp(*qp_ptr, socket);
  auto remote_qp = co_await recv_qp(socket);
  qp_ptr->rtr(remote_qp.header.lid, remote_qp.header.qp_num,
              remote_qp.header.sq_psn);
  qp_ptr->user_data() = std::move(remote_qp.user_data);
  qp_ptr->rts();
  rdma_client_func(qp_ptr).detach();
}

awaitable<void> listener() {
  auto executor = co_await this_coro::executor;
  tcp::acceptor acceptor(executor, {tcp::v4(), 55555});
  for (;;) {
    tcp::socket socket = co_await acceptor.async_accept(use_awaitable);
    co_spawn(executor, qp_acceptor(std::move(socket)), detached);
  }
}

void server_func() {
  try {
    boost::asio::io_context io_context(1);

    boost::asio::signal_set signals(io_context, SIGINT, SIGTERM);
    signals.async_wait([&](auto, auto) { io_context.stop(); });

    co_spawn(io_context, listener(), detached);

    io_context.run();
  } catch (std::exception &e) {
    BOOST_LOG_TRIVIAL(error) << "Exception: " << e.what();
  }
}

void client_func(std::string const &host, std::string const &port) {
  try {
    boost::asio::io_context io_context(1);

    boost::asio::signal_set signals(io_context, SIGINT, SIGTERM);
    signals.async_wait([&](auto, auto) { io_context.stop(); });

    auto qp_ptr = new rdmapp::qp(pd_ptr, send_cq_ptr, recv_cq_ptr);
    co_spawn(io_context, connector(host, port, qp_ptr), detached);

    io_context.run();
  } catch (std::exception &e) {
    BOOST_LOG_TRIVIAL(error) << "Exception: " << e.what();
  }
}

void cq_poller() {
  for (;;) {
    struct ibv_wc wc;
    bool ok = send_cq_ptr->poll(wc);
    if (ok) {
      auto cb_ptr = reinterpret_cast<rdmapp::executor::callback_ptr>(wc.wr_id);
      (*cb_ptr)(wc);
      rdmapp::executor::destroy_callback(cb_ptr);
    }
  }
}

int main(int argc, char *argv[]) {
  device_ptr = new rdmapp::device();
  pd_ptr = new rdmapp::pd(device_ptr);
  send_cq_ptr = new rdmapp::cq(device_ptr);
  recv_cq_ptr = send_cq_ptr;
  server_lock_state_mr =
      new rdmapp::local_mr(pd_ptr->reg_mr(&lock_state, sizeof(lock_state)));
  auto poller_thread = std::thread(cq_poller);
  if (argc == 2) {
    auto server_thread = std::thread(server_func);
    server_thread.join();
  } else if (argc == 3) {
    auto client_thread = std::thread(client_func, argv[1], argv[2]);
    client_thread.join();
  } else {
    std::cout << "Usage: " << argv[0] << " [port] for server and " << argv[0]
              << " [server_ip] [port] for client" << std::endl;
  }
  poller_thread.join();
  return 0;
}