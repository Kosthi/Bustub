//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  child_executor_->Init();
  for (uint64_t i = 0; i < plan_->limit_ && child_executor_->Next(&tuple_, &rid_);
       ++i, emit_tuples_.emplace_back(tuple_)) {
  }
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!emit_tuples_.empty()) {
    *tuple = emit_tuples_.front();
    emit_tuples_.pop_front();
    return true;
  }
  return false;
}

}  // namespace bustub
