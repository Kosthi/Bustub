//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  auto &&catalog = exec_ctx->GetCatalog();
  auto &&table_info = catalog->GetTable(plan_->table_oid_);
  table_heap_ = table_info->table_.get();
  index_info_ = catalog->GetTableIndexes(table_info->name_);
}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (inserted_ == -1) {
    return false;
  }

  while (child_executor_->Next(tuple, rid)) {
    *rid = *table_heap_->InsertTuple({0, false}, *tuple);
    for (auto &index_info : index_info_) {
      // 单键索引
      auto &&tuple_tmp = tuple->KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_,
                                             index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(tuple_tmp, *rid, exec_ctx_->GetTransaction());
    }
    ++inserted_;
  }

  std::vector<Value> value;
  value.emplace_back(INTEGER, inserted_);
  *tuple = Tuple(std::move(value), &GetOutputSchema());
  inserted_ = -1;
  return true;
}

}  // namespace bustub
