//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan_->aggregates_, plan_->agg_types_),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aggregated_) {
    return false;
  }
  AggregateValue result = aht_.GenerateInitialAggregateValue();
  if (plan_->group_bys_.empty()) {
    while (child_executor_->Next(tuple, rid)) {
      aht_.CombineAggregateValues(&result, MakeAggregateValue(tuple));
    }
  }
  *tuple = Tuple(result.aggregates_, plan_->output_schema_.get());
  return aggregated_ = true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
