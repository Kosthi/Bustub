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
}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (inserted_ == -1) {
    return false;
  }

  while (child_executor_->Next(tuple, rid)) {
    table_heap_->InsertTuple({0, false}, *tuple);
    ++inserted_;
  }

  std::vector<Value> value;
  value.emplace_back(INTEGER, inserted_);
  Column column = Column("out", INTEGER);
  std::vector<Column> columns;
  columns.emplace_back(column);
  Schema schema(columns);
  *tuple = Tuple(value, &schema);
  inserted_ = -1;
  return true;
}

}  // namespace bustub
