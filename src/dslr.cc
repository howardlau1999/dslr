#include "dslr.h"

namespace dslr {
  rdmapp::task<bool> shared_mutex::lock_shared(uint64_t txn_id) {
    co_await qp_->fetch_and_add(remote_mr_, prev_state_mr_, 1);
    auto prev = *reinterpret_cast<lock_state *>(prev_state_mr_->addr());
    if (prev.shared_max() >= kCountMax || prev.exclusive_max() >= kCountMax) {
      co_await qp_->fetch_and_add(remote_mr_, prev_state_mr_, -1);
      // Backoff
      randomBackoff();
      // HandleConflict

      co_return false;
    } else if (prev.shared_max() == kCountMax - 1) {
      reset_from_[txn_id] = lock_state(prev.exclusive_max(), kCountMax,
                                       prev.exclusive_max(), kCountMax);
    }
    if (prev.exclusive_counter() == prev.exclusive_max()) {
      co_return true;
    } else {
      // HandleConflict
      co_return false;
    }
  }

  rdmapp::task<bool> shared_mutex::lock(uint64_t txn_id) {
    co_await qp_->fetch_and_add(remote_mr_, prev_state_mr_,
                                1 << sizeof(uint16_t));
    auto prev = *reinterpret_cast<lock_state *>(prev_state_mr_->addr());
    if (prev.exclusive_max() >= kCountMax || prev.shared_max() >= kCountMax) {
      co_await qp_->fetch_and_add(remote_mr_, prev_state_mr_,
                                  0xFFFF << sizeof(uint16_t));
      // Backoff
      randomBackoff();

      // HandleConflict

      co_return false;
    } else if (prev.exclusive_max() == kCountMax - 1) {
      reset_from_[txn_id] = lock_state(kCountMax, prev.shared_max(), kCountMax,
                                       prev.shared_max());
    }
    if (prev.shared_counter() == prev.shared_max() &&
        prev.exclusive_counter() == prev.exclusive_max()) {
      co_return true;
    } else {
      // HandleConflict
      co_return false;
    }
  }

  rdmapp::task<bool> shared_mutex::unlock_shared(uint64_t txn_id) {
    co_await qp_->fetch_and_add(remote_mr_, curr_state_mr_,
                                1 << (2 * sizeof(uint16_t)));

    co_return true;
  }

  rdmapp::task<bool> shared_mutex::unlock(uint64_t txn_id) {
    co_await qp_->fetch_and_add(remote_mr_, curr_state_mr_,
                                1 << (3 * sizeof(uint16_t)));

    co_return true;
  }

  rdmapp::task<void> shared_mutex::reset_lock(uint64_t txn_id) {
    while (true) {
      co_await qp_->compare_and_swap(remote_mr_, curr_state_mr_,
                                     reset_from_[txn_id].state_, 0);
      auto curr = *reinterpret_cast<lock_state *>(curr_state_mr_->addr());
      if (curr.state_ == 0) {
        reset_from_[txn_id] = lock_state(0);
        co_return;
      }
    }
  }

  rdmapp::task<bool> shared_mutex::handle_conflict(uint64_t txn_id, lock_state prev,
                                     bool exclusive) {
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
      // TODO
      if (true) {
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
        if (cas_curr.state_ == reset_state.state_) {
          if (reset_state.exclusive_max() >= kCountMax ||
              reset_state.shared_max() >= kCountMax) {
            // TODO: Reset to 0
          }
          co_return false;
        }
      }
      auto sum = prev.exclusive_max() - curr.exclusive_counter() +
                 prev.shared_max() - curr.shared_counter();
      auto start = std::chrono::steady_clock::now();
      auto wait_delay = sum * kWaitTime;
      while (std::chrono::steady_clock::now() - start < wait_delay) {
        asm volatile("pause" ::: "memory");
      }
    }
  }
}