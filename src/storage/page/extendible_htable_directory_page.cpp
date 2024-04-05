//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  global_depth_ = 0;
  max_depth_ = max_depth;

  for (auto &page_id : bucket_page_ids_) {
    page_id = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  // key code!
  auto bucket_idx = hash & GetGlobalDepthMask();
  // return bucket_idx & GetLocalDepthMask(bucket_idx);
  return bucket_idx;
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  return bucket_idx ^ (1UL << (GetLocalDepth(bucket_idx) - 1));
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  assert(global_depth_ < max_depth_);

  ++global_depth_;
  auto size = Size();
  auto offset = size / 2;
  // only copy?
  for (uint32_t bucket_idx = offset; bucket_idx < size; ++bucket_idx) {
    local_depths_[bucket_idx] = local_depths_[bucket_idx - offset];
    bucket_page_ids_[bucket_idx] = bucket_page_ids_[bucket_idx - offset];

    //    for (uint8_t ld = 1; ld <= static_cast<uint8_t>(global_depth_); ++ld) {
    //      local_depths_[bucket_idx] = ld;
    //      auto father_bucket_idx = bucket_idx & GetLocalDepthMask(bucket_idx);
    //      if (ld == local_depths_[father_bucket_idx]) {
    //        bucket_page_ids_[bucket_idx] = bucket_page_ids_[father_bucket_idx];
    //        break;
    //      }
    //    }
  }
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  auto size = Size();
  for (uint32_t bucket_idx = size / 2; bucket_idx < size; ++bucket_idx) {
    local_depths_[bucket_idx] = 0;
    bucket_page_ids_[bucket_idx] = INVALID_PAGE_ID;
  }
  // only decr?
  --global_depth_;
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  auto size = Size();
  for (uint32_t bucket_idx = 0; bucket_idx < size; ++bucket_idx) {
    if (local_depths_[bucket_idx] >= global_depth_) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return 1UL << global_depth_; }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  ++local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  --local_depths_[bucket_idx];
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t { return (1UL << global_depth_) - 1; }

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  return (1UL << local_depths_[bucket_idx]) - 1;
}

auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t { return max_depth_; }

auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t { return 1UL << max_depth_; }

}  // namespace bustub
