//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  joined_ = false;
  left_joined_ = false;
  left_child_->Init();
  right_child_->Init();
  hash_table_.clear();
  while (left_child_->Next(&left_tuple_, &rid_)) {
    std::vector<Value> values;
    values.reserve(plan_->left_key_expressions_.size());
    for (auto &expr : plan_->left_key_expressions_) {
      values.emplace_back(expr->Evaluate(&left_tuple_, left_child_->GetOutputSchema()));
    }
    HashJoinKey key{values};
    it_ = hash_table_.find(key);
    if (it_ == hash_table_.end()) {
      hash_table_.emplace(key, std::make_pair(false, std::vector<Tuple>{left_tuple_}));
    } else {
      it_->second.second.emplace_back(left_tuple_);
    }
  }
  it_ = hash_table_.begin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!emit_tuples_.empty()) {
    *tuple = emit_tuples_.back();
    emit_tuples_.pop_back();
    return true;
  }

  if (left_joined_) {
    return false;
  }

  while (right_child_->Next(&right_tuple_, rid)) {
    std::vector<Value> values;
    values.reserve(plan_->right_key_expressions_.size());
    for (auto &expr : plan_->right_key_expressions_) {
      values.emplace_back(expr->Evaluate(&right_tuple_, right_child_->GetOutputSchema()));
    }
    HashJoinKey key{values};
    it_ = hash_table_.find(key);
    if (it_ != hash_table_.end()) {
      // 哈希击中
      it_->second.first = true;
      // 缓存一部分tuples
      for (auto &to_tuple : it_->second.second) {
        left_tuple_ = to_tuple;
        values.clear();
        auto &&left_cols = plan_->GetLeftPlan()->output_schema_->GetColumns();
        auto &&right_cols = plan_->GetRightPlan()->output_schema_->GetColumns();
        values.reserve(left_cols.size() + right_cols.size());
        for (uint64_t &&i = 0; i < left_cols.size(); ++i) {
          values.emplace_back(left_tuple_.GetValue(&left_child_->GetOutputSchema(), i));
        }
        for (uint64_t &&i = 0; i < right_cols.size(); ++i) {
          values.emplace_back(right_tuple_.GetValue(&right_child_->GetOutputSchema(), i));
        }
        emit_tuples_.emplace_back(values, plan_->output_schema_.get());
      }
      *tuple = emit_tuples_.back();
      emit_tuples_.pop_back();
      return true;
    }
  }

  if (plan_->GetJoinType() == JoinType::LEFT) {
    for (it_ = hash_table_.begin(); it_ != hash_table_.end(); ++it_) {
      // 未击中，右侧为空值
      if (!it_->second.first) {
        // 缓存一部分tuples
        std::vector<Value> values;
        for (auto &to_tuple : it_->second.second) {
          values.clear();
          left_tuple_ = to_tuple;
          auto &&left_cols = plan_->GetLeftPlan()->output_schema_->GetColumns();
          auto &&right_cols = plan_->GetRightPlan()->output_schema_->GetColumns();
          values.reserve(left_cols.size() + right_cols.size());
          for (uint64_t &&i = 0; i < left_cols.size(); ++i) {
            values.emplace_back(left_tuple_.GetValue(&left_child_->GetOutputSchema(), i));
          }
          for (const auto &col : right_cols) {
            values.emplace_back(ValueFactory::GetNullValueByType(col.GetType()));
          }
          emit_tuples_.emplace_back(values, plan_->output_schema_.get());
        }
      }
    }
    *tuple = emit_tuples_.back();
    emit_tuples_.pop_back();
    return left_joined_ = true;
  }
  return false;
}

}  // namespace bustub
