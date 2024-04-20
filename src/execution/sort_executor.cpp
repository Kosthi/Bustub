#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  emit_tuples_.clear();
  child_executor_->Init();
  while (child_executor_->Next(&tuple_, &rid_)) {
    emit_tuples_.emplace_back(tuple_);
  }
  std::sort(emit_tuples_.begin(), emit_tuples_.end(), [&](const Tuple &a, const Tuple &b) {
    for (auto &[order_type, expr] : plan_->order_bys_) {
      auto &&lhs = expr->Evaluate(&a, *plan_->output_schema_);
      auto &&rhs = expr->Evaluate(&b, *plan_->output_schema_);
      if (order_type == OrderByType::DEFAULT || order_type == OrderByType::ASC) {
        if (lhs.CompareEquals(rhs) != CmpBool::CmpTrue) {
          return lhs.CompareLessThan(rhs) == CmpBool::CmpTrue;
        }
      } else if (order_type == OrderByType::DESC) {
        if (lhs.CompareEquals(rhs) != CmpBool::CmpTrue) {
          return lhs.CompareGreaterThan(rhs) == CmpBool::CmpTrue;
        }
      } else {
        BUSTUB_ASSERT(false, "Error OrderByType.");
      }
    }
    return false;
  });
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!emit_tuples_.empty()) {
    *tuple = emit_tuples_.front();
    emit_tuples_.pop_front();
    return true;
  }
  return false;
}

}  // namespace bustub
