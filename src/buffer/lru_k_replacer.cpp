//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    size_t max_backward_k_distance = 0;
    size_t earliest_timestamp = 0; // the earliest timestamp when the backward k distance is same
    for (auto it = list_.begin(); it != list_.end(); it++) {
        if (!it->IsEvictable()) continue;
        size_t bkd = it->CalcBackwardKDistance(current_timestamp_);
        if (max_backward_k_distance < bkd) {
            *frame_id = it->GetFrameId();
            max_backward_k_distance = bkd;
        } else if (max_backward_k_distance == bkd) {
            size_t earliest_ts = it->GetEarliestAccessTimestamp();
            if (earliest_timestamp > earliest_ts) {
                *frame_id = it->GetFrameId();
                earliest_timestamp = earliest_ts;
            }
        }
    }
    if (max_backward_k_distance == 0) return false; // no frame can be evicted
    RemoveEvictableFrameById(*frame_id);
    return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    BUSTUB_ASSERT(0 <= replacer_size_ - frame_id, "invalid frame");
    if (map_.count(frame_id) == 0) { // not find, create a new frame entry
        Frame frame(frame_id, k_);
        list_.push_back(frame);
        map_[frame_id] = (--list_.end());
    }
    map_[frame_id]->RecordAccess(current_timestamp_);
    current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    BUSTUB_ASSERT(0 <= replacer_size_ - frame_id, "invalid frame");
    if (map_.count(frame_id)) { // the frame exists
        curr_size_ += map_[frame_id]->SetEvictable(set_evictable);
    }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    if (map_.count(frame_id) == 0) return; // not find
    BUSTUB_ASSERT(map_[frame_id]->IsEvictable(), "can not remove non-evictable frame");
    RemoveEvictableFrameById(frame_id);
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
