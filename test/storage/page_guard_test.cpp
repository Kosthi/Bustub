//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard_test.cpp
//
// Identification: test/storage/page_guard_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <random>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/page/page_guard.h"

#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(PageGuardTest, SampleTest) {
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  auto guarded_page = BasicPageGuard(bpm.get(), page0);

  EXPECT_EQ(page0->GetData(), guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());

  guarded_page.Drop();

  EXPECT_EQ(0, page0->GetPinCount());

  // double drop with lock checking
  {
    auto *page2 = bpm->NewPage(&page_id_temp);
    page2->RLatch();
    auto guard2 = ReadPageGuard(bpm.get(), page2);
    auto guard3 = std::move(guard2);
    guard3.Drop();
  }

  {
    auto *page3 = bpm->NewPage(&page_id_temp);
    page3->WLatch();
    auto guard4 = WritePageGuard(bpm.get(), page3);
    auto guard5 = std::move(guard4);
    guard5.Drop();
  }

  {
    auto *page4 = bpm->NewPage(&page_id_temp);
    bpm->FetchPage(page4->GetPageId());
    auto *page5 = bpm->NewPage(&page_id_temp);
    page4->WLatch();
    page5->WLatch();
    auto guard6 = WritePageGuard(bpm.get(), page4);
    auto guard7 = WritePageGuard(bpm.get(), page5);
    auto guard = std::move(guard6);
    EXPECT_EQ(2, page4->GetPinCount());
    // guard6 can not unpin because it already released
    guard6.Drop();
    EXPECT_EQ(2, page4->GetPinCount());

    // guard7 = std::move(guard);
    guard7 = WritePageGuard(std::move(guard));
    // guard7 should release its lock and decrease pin count
    EXPECT_EQ(0, page5->GetPinCount());
    page5->WLatch();
    page5->WUnlatch();

    guard7.Drop();
    EXPECT_EQ(1, page4->GetPinCount());
    EXPECT_EQ(true, bpm->UnpinPage(page4->GetPageId(), false));
  }

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

}  // namespace bustub
