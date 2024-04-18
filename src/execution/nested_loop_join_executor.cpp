//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  joined_ = false;
  left_executor_->Init();
  right_executor_->Init();
  left_executor_->Next(&left_tuple_, &rid_);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto &&filter_expr = plan_->Predicate();
  do {
    while (right_executor_->Next(&right_tuple_, rid)) {
      auto &&value = filter_expr->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple_,
                                               right_executor_->GetOutputSchema());
      if (!value.IsNull() && value.GetAs<bool>()) {
        std::vector<Value> values;
        auto &&left_cols = plan_->GetLeftPlan()->output_schema_->GetColumns();
        auto &&right_cols = plan_->GetRightPlan()->output_schema_->GetColumns();
        values.reserve(left_cols.size() + right_cols.size());
        for (uint64_t &&i = 0; i < left_cols.size(); ++i) {
          values.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (uint64_t &&i = 0; i < right_cols.size(); ++i) {
          values.emplace_back(right_tuple_.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        *tuple = Tuple(values, plan_->output_schema_.get());
        return joined_ = true;
      }
    }
    if (!joined_ && plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      auto &&left_cols = plan_->GetLeftPlan()->output_schema_->GetColumns();
      auto &&right_cols = plan_->GetRightPlan()->output_schema_->GetColumns();
      values.reserve(left_cols.size() + right_cols.size());
      for (uint64_t &&i = 0; i < left_cols.size(); ++i) {
        values.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (const auto &col : right_cols) {
        values.emplace_back(ValueFactory::GetNullValueByType(col.GetType()));
      }
      *tuple = Tuple(values, plan_->output_schema_.get());
      return joined_ = true;
    }
    right_executor_->Init();
    joined_ = false;
  } while (left_executor_->Next(&left_tuple_, rid));
  return false;
}

}  // namespace bustub
