#ifndef _DSLR_H_
#define _DSLR_H_

#include <chrono>
#include <cstdint>
#include <memory>
#include <random>
#include <unordered_map>

#include "rdmapp/qp.h"
#include "rdmapp/task.h"
#include <rdmapp/mr.h>

namespace dslr {

using namespace std::chrono_literals;

auto const kLeaseTime = 10ms;
auto const kWaitTime = 5us;

inline auto randomBackoffDuration() {
  static std::random_device rd;
  static std::mt19937 gen(rd());
  static std::uniform_int_distribution<> dis(0, 10);
  return std::chrono::milliseconds(dis(gen));
}

inline void random_backoff() {
  auto start = std::chrono::steady_clock::now();
  auto delay = randomBackoffDuration();
  while (std::chrono::steady_clock::now() - start < delay) {
    asm volatile("pause" ::: "memory");
  }
}

struct lock_state {
  uint64_t state_ = 0;
  lock_state() = default;
  explicit lock_state(uint64_t state) : state_(state) {}
  lock_state(uint16_t exclusive_counter, uint16_t shared_counter,
             uint16_t exclusive_max, uint16_t shared_max)
      : state_((static_cast<uint64_t>(exclusive_counter) << 48) |
               (static_cast<uint64_t>(shared_counter) << 32) |
               (static_cast<uint64_t>(exclusive_max) << 16) |
               static_cast<uint64_t>(shared_max)) {}
  lock_state(const lock_state &) = default;
  lock_state &operator=(const lock_state &) = default;
  lock_state(lock_state &&) = default;
  lock_state &operator=(lock_state &&) = default;
  uint16_t exclusive_counter() const { return (state_ >> 48) & 0xffff; }
  uint16_t shared_counter() const { return (state_ >> 32) & 0xffff; }
  uint16_t exclusive_max() const { return (state_ >> 16) & 0xffff; }
  uint16_t shared_max() const { return state_ & 0xffff; }
};

class shared_mutex {
private:
  rdmapp::remote_mr remote_mr_;
  std::shared_ptr<rdmapp::local_mr> prev_state_mr_;
  std::shared_ptr<rdmapp::local_mr> curr_state_mr_;
  std::shared_ptr<rdmapp::qp> qp_;
  static constexpr uint16_t kCountMax = 32768;
  std::unordered_map<uint64_t, lock_state> reset_from_;

public:
  shared_mutex(rdmapp::remote_mr remote_mr,
               std::shared_ptr<rdmapp::local_mr> prev_state_mr,
               std::shared_ptr<rdmapp::local_mr> curr_state_mr,
               std::shared_ptr<rdmapp::qp> qp)
      : remote_mr_(remote_mr), prev_state_mr_(prev_state_mr),
        curr_state_mr_(curr_state_mr), qp_(qp) {}

  rdmapp::task<bool> lock_shared(uint64_t txn_id);

  rdmapp::task<bool> lock(uint64_t txn_id);

  rdmapp::task<bool> unlock_shared(uint64_t txn_id);

  rdmapp::task<bool> unlock(uint64_t txn_id);

private:
  rdmapp::task<void> reset_lock(uint64_t txn_id);

  rdmapp::task<bool> handle_conflict(uint64_t txn_id, lock_state prev,
                                     bool exclusive);
};

} // namespace dslr

#endif // !_DSLR_H_