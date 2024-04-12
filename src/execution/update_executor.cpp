//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  auto &&catalog = exec_ctx->GetCatalog();
  auto &&table_info = catalog->GetTable(plan_->table_oid_);
  table_info_ = table_info;
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (updated_ == -1) {
    return false;
  }

  auto &table_heap = table_info_->table_;
  while (child_executor_->Next(tuple, rid)) {
    // 逻辑删除
    auto &&tuple_meta = table_heap->GetTupleMeta(*rid);
    tuple_meta.is_deleted_ = true;
    table_heap->UpdateTupleMeta(tuple_meta, *rid);

    // 创建新元组
    std::vector<Value> values{};
    values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    const auto &row_expr = plan_->target_expressions_;
    for (const auto &col : row_expr) {
      values.emplace_back(col->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    *tuple = Tuple{values, &child_executor_->GetOutputSchema()};

    // 插入
    *rid = *table_heap->InsertTuple({0, false}, *tuple);
    ++updated_;
  }

  std::vector<Value> value;
  value.emplace_back(INTEGER, updated_);
  *tuple = Tuple(std::move(value), &GetOutputSchema());
  updated_ = -1;
  return true;
}

}  // namespace bustub
