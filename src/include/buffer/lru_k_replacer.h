//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

static const size_t INF = std::numeric_limits<size_t>::max();

namespace bustub {

class Frame {
 public:
  Frame(frame_id_t id, size_t k) : id_(id), k_(k) {}

  ~Frame() = default;

  inline auto IsEvictable() -> bool { return evictable_; }

  inline auto GetFrameId() -> frame_id_t { return id_; }

  /**
   * @brief calculate the backward k distance
   * @param current_timestamp the current timestamp
   * @return if the frame has k access records, return the backward k distance calculated
   *         otherwise, return INF
   */
  auto CalcBackwardKDistance(size_t current_timestamp) -> size_t {
    if (access_history_.size() == k_) {  // has k access records
      return current_timestamp - access_history_.back();
    }
    return INF;
  }

  auto GetEarliestAccessTimestamp() -> size_t { return access_history_.back(); }

  /**
   * @brief set the frame whether evictable or not
   * @param evictable true / false
   * @return from non-evictable to evictable, return 1 (means size of replacer increment by 1)
   *         from evictable to non-evictable, return -1(means size of replacer decrement by 1)
   */
  auto SetEvictable(bool evictable) -> size_t {
    if (!evictable_ && evictable) {  // non-evictable -> evictable
      evictable_ = evictable;
      return 1;
    }
    if (evictable_ && !evictable) {  // evictable -> non-evictable
      evictable_ = evictable;
      return -1;
    }
    return 0;
  }

  void RecordAccess(size_t timestamp) {
    access_history_.push_front(timestamp);
    if (access_history_.size() > k_) {
      access_history_.pop_back();
    }
  }

  void ClearAccessHistory() { access_history_.clear(); }

 private:
  frame_id_t id_;
  bool evictable_{false};
  size_t k_;
  std::list<size_t> access_history_;
};

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict the frame with the earliest
   * timestamp overall.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   */
  void RecordAccess(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

  /**
   * @brief clear the access history of the Frame assigned by frame_id
   *
   * when do the page replacement in buffer pool, before new page insert into frame
   * the access history of old page in frame should be removed
   * and remember to set the frame non-evictable, because it will be pinned
   *
   * @param frame_id id of the frame that need to remove the access history
   */
  void ClearAccessHistory(frame_id_t frame_id);

 private:
  void RemoveEvictableFrameById(frame_id_t frame_id) {
    list_.erase(map_[frame_id]);
    map_.erase(frame_id);
    curr_size_--;
  }

  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  size_t current_timestamp_{0};
  size_t curr_size_{0};
  size_t replacer_size_;
  size_t k_;
  std::mutex latch_;
  std::unordered_map<frame_id_t, std::list<Frame>::iterator> map_;
  std::list<Frame> list_;  // double linked list
};

}  // namespace bustub
