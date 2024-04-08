//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  auto &&catalog = exec_ctx->GetCatalog();
  auto &&table_info = catalog->GetTable(plan_->table_oid_);
  table_iterator_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());
}

void SeqScanExecutor::Init() {}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_iterator_->IsEnd()) {
    return false;
  }
  auto &it = *table_iterator_;
  while (!it.IsEnd()) {
    auto &&[meta, to_tuple] = it.GetTuple();
    if (!meta.is_deleted_) {
      *tuple = std::move(to_tuple);
      *rid = it.GetRID();
      ++it;
      return true;
    }
    ++it;
  }
  return false;
}

}  // namespace bustub
