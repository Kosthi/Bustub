#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  emit_tuples_.clear();
  child_executor_->Init();
  auto comp = [&](const Tuple &a, const Tuple &b) {
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
  };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(comp)> heap(comp);

  for (std::size_t i = 0; i < plan_->n_ && child_executor_->Next(&tuple_, &rid_); ++i) {
    heap.emplace(tuple_);
  }

  while (child_executor_->Next(&tuple_, &rid_)) {
    for (auto &[order_type, expr] : plan_->order_bys_) {
      auto &&lhs = expr->Evaluate(&tuple_, *plan_->output_schema_);
      auto &&rhs = expr->Evaluate(&heap.top(), *plan_->output_schema_);
      if (order_type == OrderByType::DEFAULT || order_type == OrderByType::ASC) {
        if (lhs.CompareEquals(rhs) != CmpBool::CmpTrue) {
          if (lhs.CompareLessThan(rhs) == CmpBool::CmpTrue) {
            heap.pop();
            heap.push(tuple_);
          }
          break;
        }
      } else if (order_type == OrderByType::DESC) {
        if (lhs.CompareEquals(rhs) != CmpBool::CmpTrue) {
          if (lhs.CompareGreaterThan(rhs) == CmpBool::CmpTrue) {
            heap.pop();
            heap.push(tuple_);
          }
          break;
        }
      } else {
        BUSTUB_ASSERT(false, "Error OrderByType.");
      }
    }
  }

  while (!heap.empty()) {
    emit_tuples_.emplace_back(heap.top());
    heap.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!emit_tuples_.empty()) {
    *tuple = emit_tuples_.back();
    emit_tuples_.pop_back();
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return emit_tuples_.size(); }

}  // namespace bustub
