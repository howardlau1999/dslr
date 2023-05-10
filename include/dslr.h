#ifndef _DSLR_H_
#define _DSLR_H_

#include <cstdint>
#include <memory>
#include <unordered_map>

#include "rdmapp/qp.h"
#include "rdmapp/task.h"
#include <rdmapp/mr.h>

namespace dslr {

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

  rdmapp::task<bool> lock_shared(uint64_t txn_id) {
    co_await qp_->fetch_and_add(remote_mr_, prev_state_mr_, 1);
    auto prev = *reinterpret_cast<lock_state *>(prev_state_mr_->addr());
    if (prev.shared_max() >= kCountMax || prev.exclusive_max() >= kCountMax) {
      co_await qp_->fetch_and_add(remote_mr_, prev_state_mr_, -1);
      // Backoff

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

  rdmapp::task<bool> lock(uint64_t txn_id) {
    co_await qp_->fetch_and_add(remote_mr_, prev_state_mr_,
                                1 << sizeof(uint16_t));
    auto prev = *reinterpret_cast<lock_state *>(prev_state_mr_->addr());
    if (prev.exclusive_max() >= kCountMax || prev.shared_max() >= kCountMax) {
      co_await qp_->fetch_and_add(remote_mr_, prev_state_mr_,
                                  0xFFFF << sizeof(uint16_t));
      // Backoff

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

  rdmapp::task<bool> unlock_shared(uint64_t txn_id) {
    co_await qp_->fetch_and_add(remote_mr_, curr_state_mr_,
                                1 << (2 * sizeof(uint16_t)));

    co_return true;
  }

  rdmapp::task<bool> unlock(uint64_t txn_id) {
    co_await qp_->fetch_and_add(remote_mr_, curr_state_mr_,
                                1 << (3 * sizeof(uint16_t)));

    co_return true;
  }

private:
  rdmapp::task<void> reset_lock(uint64_t txn_id) {
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
  
  rdmapp::task<bool> handle_conflict(uint64_t txn_id, lock_state prev,
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
      // TODO: wait
    }
  }
};

} // namespace dslr

#endif // !_DSLR_H_