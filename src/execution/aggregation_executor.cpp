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

void AggregationExecutor::Init() {
  // 一定要初始化所有成员变量 因为在join中可能会被多次调用
  empty_table_ = false;
  aht_.Clear();
  aht_iterator_ = aht_.Begin();
  child_executor_->Init();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (empty_table_) {
    return false;
  }
  if (!aht_.IsEmpty()) {
    if (aht_iterator_ == aht_.End()) {
      return false;
    }
    std::vector<Value> values;
    for (auto &v : aht_iterator_.Key().group_bys_) {
      values.emplace_back(v);
    }
    for (auto &v : aht_iterator_.Val().aggregates_) {
      values.emplace_back(v);
    }
    *tuple = Tuple(values, plan_->output_schema_.get());
    ++aht_iterator_;
    return true;
  }
  while (child_executor_->Next(tuple, rid)) {
    aht_.InsertCombine(MakeAggregateKey(tuple), MakeAggregateValue(tuple));
  }
  aht_iterator_ = aht_.Begin();
  // 空表
  if (aht_.IsEmpty()) {
    // no groups, no output
    if (!plan_->group_bys_.empty()) {
      return false;
    }
    *tuple = Tuple(aht_.GenerateInitialAggregateValue().aggregates_, plan_->output_schema_.get());
    return empty_table_ = true;
  }
  std::vector<Value> values;
  for (auto &v : aht_iterator_.Key().group_bys_) {
    values.emplace_back(v);
  }
  for (auto &v : aht_iterator_.Val().aggregates_) {
    values.emplace_back(v);
  }
  *tuple = Tuple(values, plan_->output_schema_.get());
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
