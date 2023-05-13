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

namespace bustub {

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

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  size_t current_timestamp_{0};
  size_t curr_size_{0};
  size_t replacer_size_;
  size_t k_;
  std::mutex latch_;
  std::unordered_map<frame_id_t, Frame*> map_;
  std::list<Frame*> list_; // double linked list
};

class Frame {
  public:
    Frame(frame_id_t id, size_t k) : id_(id) {
      access_history_ = new AccessHistory(k);
    }

    inline bool IsEvictable() { return evictable_; }

    size_t SetEvictable(bool evictable) {
      if (!evictable_ && evictable) {
        evictable_ = evictable;
        return 1;
      } else if (evictable_ && !evictable) {
        evictable_ = evictable;
        return -1;
      }
    }

    void RecordAccess(size_t timestamp) {
      access_history_->AddAccessRecord(timestamp);
    }

  private:
    frame_id_t id_;
    bool evictable_{false};
    long backward_k_distance_{0};
    AccessHistory* access_history_;
};

class AccessHistory {
  public:
    AccessHistory(size_t k) : k_(k) {
      head = new Entry(-1);
      tail = new Entry(-1);
      head->SetNext(tail);
      tail->SetPrev(head);
    }

    inline size_t Size() { return size_; }

    size_t GetKthAccessTimestamp() {
      if (size_ < k_) return std::numeric_limits<size_t>::max(); // less than k, return +inf
      return tail->GetPrev()->GetTimestamp();
    }

    void AddAccessRecord(size_t timestamp) {
      Entry* entry = new Entry(timestamp);
      // insert into head
      head->GetNext()->SetPrev(entry);
      entry->SetNext(head->GetNext());
      head->SetNext(entry);
      entry->SetPrev(head);
      // increment the size_
      size_++;
      if (size_ <= k_) {
        return;
      }
      // remove the last entry
      Entry* tmp = tail->GetPrev();
      tmp->GetPrev()->SetNext(tail);
      tail->SetPrev(tmp->GetPrev());
      size_--;
      delete(tmp);
    }

  class Entry {
    public:
      Entry(size_t timestamp) : timestamp_(timestamp) {}
      inline void SetNext(Entry* next) { next_ = next; }
      inline void SetPrev(Entry* prev) { prev_ = prev; }
      inline Entry* GetNext() { return next_; }
      inline Entry* GetPrev() { return prev_; }
      inline size_t GetTimestamp() { return timestamp_; }
    private:
      size_t timestamp_;
      Entry* next_{nullptr};
      Entry* prev_{nullptr};
  };

  private:
    Entry* head;
    Entry* tail;
    size_t size_{0};
    size_t k_; // record k access
};

}  // namespace bustub
