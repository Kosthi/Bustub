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
  index_info_ = catalog->GetTableIndexes(table_info->name_);
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

    // 删除索引
    for (auto &index_info : index_info_) {
      // 单键索引
      auto &&tuple_tmp = tuple->KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_,
                                             index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(tuple_tmp, *rid, exec_ctx_->GetTransaction());
    }

    ++deleted_;
  }

  std::vector<Value> value;
  value.emplace_back(INTEGER, deleted_);
  *tuple = Tuple(std::move(value), &GetOutputSchema());
  deleted_ = -1;
  return true;
}

}  // namespace bustub
