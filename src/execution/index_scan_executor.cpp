//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  auto &&cate_log = exec_ctx->GetCatalog();
  table_heap_ = cate_log->GetTable(plan_->table_oid_)->table_.get();
  index_ = cate_log->GetIndex(plan_->index_oid_)->index_.get();
  // index里已经包含了key_attrs 直接生成键模式
  key_schema_ = std::make_shared<Schema>(Schema::CopySchema(plan_->output_schema_.get(), index_->GetKeyAttrs()));
}

void IndexScanExecutor::Init() { is_executed_ = false; }

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_executed_) {
    return false;
  }

  std::vector<RID> result;
  std::vector<Value> values;
  values.emplace_back(plan_->filter_predicate_->GetChildAt(1)->Evaluate(nullptr, GetOutputSchema()));
  *tuple = Tuple(values, key_schema_.get());
  index_->ScanKey(*tuple, &result, exec_ctx_->GetTransaction());
  if (result.size() == 1) {
    *rid = result[0];
    *tuple = table_heap_->GetTuple(*rid).second;
    return is_executed_ = true;
  }
  return false;
}

}  // namespace bustub
