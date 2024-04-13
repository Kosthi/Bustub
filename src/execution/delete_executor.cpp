//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  auto &&catalog = exec_ctx->GetCatalog();
  auto &&table_info = catalog->GetTable(plan_->table_oid_);
  table_heap_ = table_info->table_.get();
}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (deleted_ == -1) {
    return false;
  }

  while (child_executor_->Next(tuple, rid)) {
    // 逻辑删除
    auto &&tuple_meta = table_heap_->GetTupleMeta(*rid);
    tuple_meta.is_deleted_ = true;
    table_heap_->UpdateTupleMeta(tuple_meta, *rid);
    ++deleted_;
  }

  std::vector<Value> value;
  value.emplace_back(INTEGER, deleted_);
  *tuple = Tuple(std::move(value), &GetOutputSchema());
  deleted_ = -1;
  return true;
}

}  // namespace bustub
