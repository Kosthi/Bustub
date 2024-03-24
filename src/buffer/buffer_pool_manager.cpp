//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  //  throw NotImplementedException(
  //      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //      "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t fid;
  if (!free_list_.empty()) {
    fid = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&fid)) {
      return nullptr;
    }
    auto &&page = pages_[fid];
    if (page.IsDirty()) {
      std::promise<bool> callback;
      auto &&future = callback.get_future();
      disk_scheduler_->Schedule(DiskRequest{true, page.data_, page.page_id_, std::move(callback)});
      future.get();
    }

    page_table_.erase(page.page_id_);
  }

  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);

  pages_[fid].ResetMemory();
  pages_[fid].page_id_ = *page_id = AllocatePage();
  pages_[fid].pin_count_ = 1;
  pages_[fid].is_dirty_ = false;

  page_table_.emplace(*page_id, fid);
  return &pages_[fid];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  auto &&it = page_table_.find(page_id);

  if (it == page_table_.end()) {
    frame_id_t fid;
    if (!free_list_.empty()) {
      fid = free_list_.front();
      free_list_.pop_front();
    } else {
      if (!replacer_->Evict(&fid)) {
        return nullptr;
      }
      auto &&page = pages_[fid];
      if (page.IsDirty()) {
        // printf("FetchPage: %s\n", pages_[fid].data_);
        std::promise<bool> callback;
        auto &&future = callback.get_future();
        disk_scheduler_->Schedule(DiskRequest{true, page.data_, page.page_id_, std::move(callback)});
        future.get();
        page.is_dirty_ = false;
      }
      page_table_.erase(page.page_id_);
    }

    auto &&page = pages_[fid];
    page.ResetMemory();
    page.page_id_ = page_id;
    page.pin_count_ = 0;
    page.is_dirty_ = false;

    std::promise<bool> callback;
    auto &&future = callback.get_future();
    disk_scheduler_->Schedule(DiskRequest{false, page.data_, page.page_id_, std::move(callback)});
    future.get();

    it = page_table_.emplace(page.page_id_, fid).first;
  }

  ++pages_[it->second].pin_count_;
  replacer_->RecordAccess(it->second);
  replacer_->SetEvictable(it->second, false);
  return &pages_[it->second];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  auto &&it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }

  auto &&page = pages_[it->second];
  if (page.pin_count_ == 0) {
    return false;
  }

  if (--page.pin_count_ == 0) {
    replacer_->SetEvictable(it->second, true);
  }

  page.is_dirty_ |= is_dirty;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  auto &&it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }

  auto &&page = pages_[it->second];

  std::promise<bool> callback;
  auto &&future = callback.get_future();
  disk_scheduler_->Schedule(DiskRequest{true, page.data_, page.page_id_, std::move(callback)});
  future.get();

  page.is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(latch_);

  for (auto &&it : page_table_) {
    auto &&page = pages_[it.second];

    std::promise<bool> callback;
    auto &&future = callback.get_future();
    disk_scheduler_->Schedule(DiskRequest{true, page.data_, page.page_id_, std::move(callback)});
    future.get();

    page.is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  auto &&it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }

  auto &&page = pages_[it->second];
  if (page.pin_count_ > 0) {
    return false;
  }

  // reset metadata of page
  page.ResetMemory();
  page.pin_count_ = 0;
  page.is_dirty_ = false;
  page.page_id_ = INVALID_PAGE_ID;

  replacer_->Remove(it->second);
  free_list_.push_back(it->second);
  page_table_.erase(it);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
