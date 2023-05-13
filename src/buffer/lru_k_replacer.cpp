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

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { return false; }

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    map_[frame_id]->RecordAccess(current_timestamp_);
    current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    curr_size_ += map_[frame_id]->SetEvictable(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    if (map_.count(frame_id) == 0) return; // not find
    if (!map_[frame_id]->IsEvictable()) {}
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
