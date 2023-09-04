#include "dslr.h"

#include <boost/log/trivial.hpp>

namespace dslr {

rdmapp::task<bool> shared_mutex::lock_shared(uint64_t txn_id) {
  int nr_conflicts = 0;
  reset_from_[txn_id] = lock_state(0);
  co_await qp_->fetch_and_add(remote_mr_, prev_state_mr_, 1ULL);
  auto prev = *reinterpret_cast<lock_state *>(prev_state_mr_->addr());
  BOOST_LOG_TRIVIAL(info) << "prev=" << std::string(prev);
  if (prev.shared_max() >= kCountMax || prev.exclusive_max() >= kCountMax) {
    constexpr uint64_t neg = -1;
    co_await qp_->fetch_and_add(remote_mr_, prev_state_mr_, neg);
    // Backoff
    {
      random_backoff(nr_conflicts);
      ++nr_conflicts;
    }

    // TODO: prev ns or prev nx has not changed for longer than twice the lease
    // time

    co_return false;
  } else if (prev.shared_max() == kCountMax - 1) {
    reset_from_[txn_id] = lock_state(prev.exclusive_max(), kCountMax,
                                     prev.exclusive_max(), kCountMax);
  }
  if (prev.exclusive_counter() == prev.exclusive_max()) {
    BOOST_LOG_TRIVIAL(info) << "lock_shared success";
    co_return true;
  } else {
    // HandleConflict
    co_return co_await handle_conflict(txn_id, prev, false);
  }
}

rdmapp::task<bool> shared_mutex::lock(uint64_t txn_id) {
  int nr_conflicts = 0;
  reset_from_[txn_id] = lock_state(0);
  co_await qp_->fetch_and_add(remote_mr_, prev_state_mr_,
                              1ULL << kExclusiveMaxBitOffset);
  auto prev = *reinterpret_cast<lock_state *>(prev_state_mr_->addr());
  if (prev.exclusive_max() >= kCountMax || prev.shared_max() >= kCountMax) {
    constexpr uint64_t neg = -(1ULL << kExclusiveMaxBitOffset);
    co_await qp_->fetch_and_add(remote_mr_, prev_state_mr_, neg);
    {
      random_backoff(nr_conflicts);
      ++nr_conflicts;
    }

    // TODO: prev ns or prev nx has not changed for longer than twice the lease
    // time

    co_return false;
  } else if (prev.exclusive_max() == kCountMax - 1) {
    reset_from_[txn_id] =
        lock_state(kCountMax, prev.shared_max(), kCountMax, prev.shared_max());
  }
  if (prev.shared_counter() == prev.shared_max() &&
      prev.exclusive_counter() == prev.exclusive_max()) {
    co_return true;
  } else {
    // HandleConflict
    co_return co_await handle_conflict(txn_id, prev, true);
  }
}

rdmapp::task<bool>
shared_mutex::unlock_shared(uint64_t txn_id,
                            std::chrono::microseconds const elapsed) {
  auto const reset_from = reset_from_[txn_id];
  if (elapsed < kLeaseTime || reset_from.state_ != 0) {
    co_await qp_->fetch_and_add(remote_mr_, curr_state_mr_,
                                1ULL << kSharedCounterBitOffset);
    auto const curr = *reinterpret_cast<lock_state *>(curr_state_mr_->addr());
    BOOST_LOG_TRIVIAL(info) << "curr=" << std::string(curr);
    if (reset_from.state_ > 0) {
      co_await reset_lock(txn_id);
    }
  }

  co_return true;
}

rdmapp::task<bool>
shared_mutex::unlock(uint64_t txn_id, std::chrono::microseconds const elapsed) {
  auto const reset_from = reset_from_[txn_id];
  if (elapsed < kLeaseTime || reset_from.state_ != 0) {
    co_await qp_->fetch_and_add(remote_mr_, curr_state_mr_,
                                1ULL << kExclusiveCounterBitOffset);

    if (reset_from.state_ > 0) {
      co_await reset_lock(txn_id);
    }
  }

  co_return true;
}

rdmapp::task<void> shared_mutex::reset_lock(uint64_t txn_id) {
  while (true) {
    auto const reset_from = reset_from_[txn_id];
    co_await qp_->compare_and_swap(remote_mr_, curr_state_mr_,
                                   reset_from.state_, 0ULL);
    // RDMA CAS always returns the old value of the remote memory
    // If the returned value is the same as reset_from, then the CAS succeeded
    auto curr = *reinterpret_cast<lock_state *>(curr_state_mr_->addr());
    if (curr.state_ == reset_from.state_) {
      // CAS succeeded
      reset_from_[txn_id] = lock_state(0);
      co_return;
    }
  }
}

rdmapp::task<bool> shared_mutex::handle_conflict(uint64_t txn_id,
                                                 lock_state prev,
                                                 bool exclusive) {
  uint32_t last_shared_counter = 1 << 17;
  auto last_shared_ts = std::chrono::steady_clock::now();
  uint32_t last_exclusive_counter = 1 << 17;
  auto last_exclusive_ts = last_shared_ts;

  while (true) {
    co_await qp_->read(remote_mr_, curr_state_mr_);
    auto curr = *reinterpret_cast<lock_state *>(curr_state_mr_->addr());
    if (!exclusive) {
      if (prev.exclusive_max() == curr.exclusive_counter()) {
        co_return true;
      }
    } else {
      if (prev.exclusive_max() == curr.exclusive_counter() &&
          prev.shared_max() == curr.shared_counter()) {
        co_return true;
      }
    }
    if (curr.shared_counter() != last_shared_counter) {
      last_shared_counter = curr.shared_counter();
      last_shared_ts = std::chrono::steady_clock::now();
    }
    if (curr.exclusive_counter() != last_exclusive_counter) {
      last_exclusive_counter = curr.exclusive_counter();
      last_exclusive_ts = std::chrono::steady_clock::now();
    }
    auto const elapsed_shared =
        std::chrono::steady_clock::now() - last_shared_ts;
    auto const elapsed_exclusive =
        std::chrono::steady_clock::now() - last_exclusive_ts;
    if (elapsed_shared > kDoubleLeaseTime ||
        elapsed_exclusive > kDoubleLeaseTime) {
      lock_state reset_state;
      if (!exclusive) {
        reset_state = lock_state(prev.exclusive_max(), prev.shared_max() + 1,
                                 curr.exclusive_max(), curr.shared_max());
      } else {
        reset_state = lock_state(prev.exclusive_max() + 1, prev.shared_max(),
                                 curr.exclusive_max(), curr.shared_max());
      }
      co_await qp_->compare_and_swap(remote_mr_, curr_state_mr_, prev.state_,
                                     reset_state.state_);
      auto cas_curr = *reinterpret_cast<lock_state *>(curr_state_mr_->addr());
      if (cas_curr.state_ == prev.state_) {
        // CAS succeeded
        if (reset_state.exclusive_max() >= kCountMax ||
            reset_state.shared_max() >= kCountMax) {
          co_await reset_lock(txn_id);
        }
        co_return false;
      }
    }
    auto const sum = prev.exclusive_max() - curr.exclusive_counter() +
                     prev.shared_max() - curr.shared_counter();
    auto const wait_start = std::chrono::steady_clock::now();
    auto const wait_delay = sum * kWaitTime;
    while (std::chrono::steady_clock::now() - wait_start < wait_delay) {
      asm volatile("pause" ::: "memory");
    }
  }
}

} // namespace dslr