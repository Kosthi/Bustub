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
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t fid = -1;
  size_t min_time_stamp1 = std::numeric_limits<size_t>::max();
  size_t min_time_stamp2 = std::numeric_limits<size_t>::max();

  for (auto &&it : node_store_) {
    auto &&node = it.second;
    if (node.is_evictable_) {
      if (node.history_.size() < k_ && node.history_.front() < min_time_stamp1) {
        min_time_stamp1 = node.history_.front();
        fid = it.first;
        min_time_stamp2 = 0;
      } else if (node.history_.size() == k_ && node.history_.front() < min_time_stamp2) {
        min_time_stamp2 = node.history_.front();
        fid = it.first;
      }
    }
  }

  if (fid == -1) {
    return false;
  }

  --curr_size_;
  node_store_.erase(fid);
  *frame_id = fid;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);

  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw ExecutionException("[LRUKReplacer] The frame id is invalid when calling RecordAccess function.");
  }

  auto &&it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    it = node_store_.emplace(frame_id, LRUKNode()).first;
  }
  if (it->second.history_.size() == k_) {
    it->second.history_.pop_front();
  }
  it->second.history_.emplace_back(current_timestamp_++);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);

  auto &&it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    throw ExecutionException("[LRUKReplacer] The frame id is invalid when calling SetEvictable function.");
  }
  if (it->second.is_evictable_ && !set_evictable) {
    --curr_size_;
  } else if (!it->second.is_evictable_ && set_evictable) {
    ++curr_size_;
  }
  it->second.is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  auto &&it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }

  if (!it->second.is_evictable_) {
    throw ExecutionException("[LRUKReplacer] Remove is called on a non-evictable frame when calling Remove function.");
  }

  --curr_size_;
  node_store_.erase(it);
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
