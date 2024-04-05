//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  auto &&head_page_guard = bpm_->NewPageGuarded(&header_page_id_).UpgradeWrite();
  auto &&header = head_page_guard.template AsMut<ExtendibleHTableHeaderPage>();
  header->Init(header_max_depth_);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  auto head_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header = head_page_guard.template As<ExtendibleHTableHeaderPage>();

  auto hash = Hash(key);
  auto directory_index = header->HashToDirectoryIndex(hash);
  if (header->GetDirectoryPageId(directory_index) == INVALID_PAGE_ID) {
    return false;
  }

  auto directory_page_guard = bpm_->FetchPageRead(header->GetDirectoryPageId(directory_index));
  head_page_guard.Drop();

  const auto *directory = directory_page_guard.template As<ExtendibleHTableDirectoryPage>();
  auto bucket_index = directory->HashToBucketIndex(hash);
  if (directory->GetBucketPageId(bucket_index) == INVALID_PAGE_ID) {
    return false;
  }

  auto bucket_page_guard = bpm_->FetchPageRead(directory->GetBucketPageId(bucket_index));
  directory_page_guard.Drop();

  const auto *bucket = bucket_page_guard.template As<ExtendibleHTableBucketPage<K, V, KC>>();

  V value;
  if (bucket->Lookup(key, value, cmp_)) {
    result->emplace_back(value);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  // rid(page, slot) 为 value
  auto &&head_page_guard = bpm_->FetchPageWrite(header_page_id_);
  auto &&header = head_page_guard.template AsMut<ExtendibleHTableHeaderPage>();

  auto hash = Hash(key);
  auto directory_index = header->HashToDirectoryIndex(hash);
  if (header->GetDirectoryPageId(directory_index) == INVALID_PAGE_ID) {
    return InsertToNewDirectory(header, directory_index, hash, key, value);
  }

  WritePageGuard directory_page_guard = bpm_->FetchPageWrite(header->GetDirectoryPageId(directory_index));
  head_page_guard.Drop();

  auto directory = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_index = directory->HashToBucketIndex(hash);

  if (directory->GetBucketPageId(bucket_index) == INVALID_PAGE_ID) {
    return InsertToNewBucket(directory, bucket_index, key, value);
  }

  WritePageGuard bucket_page_guard = bpm_->FetchPageWrite(directory->GetBucketPageId(bucket_index));
  auto *bucket = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  V v;
  if (bucket->Lookup(key, v, cmp_)) {
    return false;
  }

  while (bucket->IsFull()) {
    if (directory->GetLocalDepth(bucket_index) == directory->GetGlobalDepth()) {
      // directory is full
      if (directory->Size() == directory->MaxSize()) {
        return false;
      }

      directory->IncrGlobalDepth();
    }

    directory->IncrLocalDepth(bucket_index);
    auto &&split_image_index = directory->GetSplitImageIndex(bucket_index);

    directory->SetLocalDepth(split_image_index, directory->GetLocalDepth(bucket_index));

    auto new_bucket_page_id = INVALID_PAGE_ID;
    auto &&new_bucket_page_guard = bpm_->NewPageGuarded(&new_bucket_page_id).UpgradeWrite();
    auto &&new_bucket = new_bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    new_bucket->Init(bucket_max_size_);

    auto &&local_depth_mask = directory->GetLocalDepthMask(split_image_index);
    auto &&new_bucket_idx = split_image_index & local_depth_mask;
    auto &&new_local_depth = directory->GetLocalDepth(split_image_index);

    UpdateDirectoryMapping(directory, new_bucket_idx, new_bucket_page_id, new_local_depth, local_depth_mask);
    MigrateEntries(bucket, new_bucket, new_bucket_idx, local_depth_mask);

    bucket_index = directory->HashToBucketIndex(hash);

    if (directory->GetBucketPageId(bucket_index) == INVALID_PAGE_ID) {
      return InsertToNewBucket(directory, bucket_index, key, value);
    }

    // 恰好是新创建的页面，如果是原来的页面那么不用改
    if (directory->GetBucketPageId(bucket_index) == new_bucket_page_id) {
      bucket_page_guard = std::move(new_bucket_page_guard);
      bucket = new_bucket;
    } else if (directory->GetBucketPageId(bucket_index) != bucket_page_guard.PageId()) {
      bucket_page_guard = bpm_->FetchPageWrite(directory->GetBucketPageId(bucket_index));
      bucket = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    }
  }
  directory_page_guard.Drop();
  return bucket->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t directory_page_id;
  WritePageGuard directory_page_guard = bpm_->NewPageGuarded(&directory_page_id).UpgradeWrite();
  header->SetDirectoryPageId(directory_idx, directory_page_id);

  auto directory = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();
  directory->Init(directory_max_depth_);

  auto bucket_index = directory->HashToBucketIndex(hash);
  if (directory->GetBucketPageId(bucket_index) == INVALID_PAGE_ID) {
    return InsertToNewBucket(directory, bucket_index, key, value);
  }

  // never reach
  WritePageGuard bucket_page_guard = bpm_->FetchPageWrite(directory->GetBucketPageId(bucket_index));
  auto *bucket = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  return bucket->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t bucket_page_id;
  WritePageGuard bucket_page_guard = bpm_->NewPageGuarded(&bucket_page_id).UpgradeWrite();
  directory->SetBucketPageId(bucket_idx, bucket_page_id);

  auto *bucket = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket->Init(bucket_max_size_);
  return bucket->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  directory->SetLocalDepth(new_bucket_idx, new_local_depth);
  directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t new_bucket_idx, uint32_t local_depth_mask) {
  for (uint32_t bucket_idx = 0; bucket_idx < old_bucket->Size(); ++bucket_idx) {
    auto &&key = old_bucket->KeyAt(bucket_idx);
    auto &&value = old_bucket->ValueAt(bucket_idx);
    auto &&hash = Hash(key);
    if ((hash & local_depth_mask) == new_bucket_idx) {
      new_bucket->Insert(key, value, cmp_);
      old_bucket->RemoveAt(bucket_idx);
      --bucket_idx;
    }
  }
}

// template <typename K, typename V, typename KC>
// void DiskExtendibleHashTable<K, V, KC>::ReInsert()
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  auto &&head_page_guard = bpm_->FetchPageWrite(header_page_id_);
  auto &&header = head_page_guard.template AsMut<ExtendibleHTableHeaderPage>();

  auto hash = Hash(key);
  auto directory_index = header->HashToDirectoryIndex(hash);
  if (header->GetDirectoryPageId(directory_index) == INVALID_PAGE_ID) {
    return false;
  }

  WritePageGuard directory_page_guard = bpm_->FetchPageWrite(header->GetDirectoryPageId(directory_index));
  // head_page_guard.Drop();

  auto directory = directory_page_guard.template AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_index = directory->HashToBucketIndex(hash);

  if (directory->GetBucketPageId(bucket_index) == INVALID_PAGE_ID) {
    return false;
  }

  WritePageGuard bucket_page_guard = bpm_->FetchPageWrite(directory->GetBucketPageId(bucket_index));
  auto *bucket = bucket_page_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  if (!bucket->Remove(key, cmp_)) {
    return false;
  }

  while (bucket->IsEmpty()) {
    auto &&target_page_id = directory->GetBucketPageId(bucket_index);
    auto &&target_local_depth = directory->GetLocalDepth(bucket_index);

    if (directory->GetGlobalDepth() == 0) {
      bucket_page_guard.Drop();
      assert(bpm_->DeletePage(target_page_id));

      directory->SetBucketPageId(bucket_index, INVALID_PAGE_ID);
      directory->SetLocalDepth(bucket_index, 0);
      header->SetDirectoryPageId(directory_index, INVALID_PAGE_ID);

      return true;
    }

    auto &&split_image_index = directory->GetSplitImageIndex(bucket_index);
    auto &&split_image_page_id = directory->GetBucketPageId(split_image_index);

    if (target_local_depth > 0 && directory->GetLocalDepth(split_image_index) == target_local_depth) {
      for (uint32_t i = 0; i < directory->Size(); ++i) {
        if (i != split_image_index && directory->GetBucketPageId(i) == target_page_id) {
          directory->SetBucketPageId(i, split_image_page_id);
          directory->DecrLocalDepth(i);
        }
      }
      directory->DecrLocalDepth(split_image_index);
      if (directory->CanShrink()) {
        directory->DecrGlobalDepth();
        // 如果image被删除了，应该重新计算
        if (directory->GetGlobalDepth() == 0) {
          bucket_page_guard.Drop();
          assert(bpm_->DeletePage(target_page_id));

          // directory->SetBucketPageId(bucket_index, INVALID_PAGE_ID);
          // directory->SetLocalDepth(bucket_index, 0);
          // header->SetDirectoryPageId(directory_index, INVALID_PAGE_ID);
          split_image_index = 0;
          split_image_page_id = directory->GetBucketPageId(0);
        } else if (directory->GetLocalDepth(bucket_index) > 0) {
          split_image_index = directory->GetSplitImageIndex(bucket_index);
          split_image_page_id = directory->GetBucketPageId(split_image_index);
        }
      }

      bucket_index = split_image_index;
      bucket_page_guard = bpm_->FetchPageWrite(split_image_page_id);
      bucket = bucket_page_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    } else {
      return true;
    }
  }
  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
