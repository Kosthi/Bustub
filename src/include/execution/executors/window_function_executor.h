//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.h
//
// Identification: src/include/execution/executors/window_function_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/executors/sort_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The WindowFunctionExecutor executor executes a window function for columns using window function.
 *
 * Window function is different from normal aggregation as it outputs one row for each inputing rows,
 * and can be combined with normal selected columns. The columns in WindowFunctionPlanNode contains both
 * normal selected columns and placeholder columns for window functions.
 *
 * For example, if we have a query like:
 *    SELECT 0.1, 0.2, SUM(0.3) OVER (PARTITION BY 0.2 ORDER BY 0.3), SUM(0.4) OVER (PARTITION BY 0.1 ORDER BY 0.2,0.3)
 *      FROM table;
 *
 * The WindowFunctionPlanNode contains following structure:
 *    columns: std::vector<AbstractExpressionRef>{0.1, 0.2, 0.-1(placeholder), 0.-1(placeholder)}
 *    window_functions_: {
 *      3: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.2}
 *        order_by: std::vector<AbstractExpressionRef>{0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.3}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *      4: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.1}
 *        order_by: std::vector<AbstractExpressionRef>{0.2,0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.4}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *    }
 *
 * Your executor should use child executor and exprs in columns to produce selected columns except for window
 * function columns, and use window_agg_indexes, partition_bys, order_bys, functionss and window_agg_types to
 * generate window function columns results. Directly use placeholders for window function columns in columns is
 * not allowed, as it contains invalid column id.
 *
 * Your WindowFunctionExecutor does not need to support specified window frames (eg: 1 preceding and 1 following).
 * You can assume that all window frames are UNBOUNDED FOLLOWING AND CURRENT ROW when there is ORDER BY clause, and
 * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING when there is no ORDER BY clause.
 *
 */
class WindowFunctionExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new WindowFunctionExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The window aggregation plan to be executed
   */
  WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the window aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the window aggregation.
   * @param[out] tuple The next tuple produced by the window aggregation
   * @param[out] rid The next tuple RID produced by the window aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the window aggregation plan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  // for std::map
  struct Compare {
    auto operator()(const AggregateKey &a, const AggregateKey &b) const -> bool {
      for (uint32_t i = 0; i < b.group_bys_.size(); i++) {
        if (a.group_bys_[i].CompareEquals(b.group_bys_[i]) != CmpBool::CmpTrue) {
          return a.group_bys_[i].CompareLessThan(b.group_bys_[i]) == CmpBool::CmpTrue;
        }
      }
      return false;
    }
  };

  /**
   * A simplified hash table that has all the necessary functionality for aggregations.
   */
  class AggregationHashTable {
   public:
    /**
     * Construct a new SimpleAggregationHashTable instance.
     * @param agg_exprs the aggregation expressions
     * @param agg_types the types of aggregations
     */
    AggregationHashTable(std::vector<AbstractExpressionRef> agg_exprs, std::vector<WindowFunctionType> agg_types)
        : agg_exprs_(std::move(agg_exprs)), agg_types_(std::move(agg_types)) {}

    /** @return The initial aggregate value for this aggregation executor */
    auto GenerateInitialAggregateValue() -> AggregateValue {
      std::vector<Value> values{};
      for (const auto &agg_type : agg_types_) {
        switch (agg_type) {
          case WindowFunctionType::CountStarAggregate:
            // Count start starts at zero.
            values.emplace_back(ValueFactory::GetIntegerValue(0));
            break;
          case WindowFunctionType::CountAggregate:
          case WindowFunctionType::SumAggregate:
          case WindowFunctionType::MinAggregate:
          case WindowFunctionType::MaxAggregate:
          case WindowFunctionType::Rank:
            // Others starts at null.
            values.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
            break;
        }
      }
      return {values};
    }

    /**
     * TODO(Student)
     *
     * Combines the input into the aggregation result.
     * @param[out] result The output aggregate value
     * @param input The input value
     */
    void CombineAggregateValues(AggregateValue *result, const AggregateValue &input) {
      for (uint32_t i = 0; i < agg_exprs_.size(); i++) {
        switch (agg_types_[i]) {
          case WindowFunctionType::CountStarAggregate:
            result->aggregates_[i] = result->aggregates_[i].Add(input.aggregates_[i]);
            break;
          case WindowFunctionType::CountAggregate:
            if (input.aggregates_[i].IsNull()) {
              break;
            }
            if (result->aggregates_[i].IsNull()) {
              result->aggregates_[i] = {TypeId::INTEGER, 1};
            } else {
              result->aggregates_[i] = result->aggregates_[i].Add({TypeId::INTEGER, 1});
            }
            break;
          case WindowFunctionType::SumAggregate:
            if (input.aggregates_[i].IsNull()) {
              break;
            }
            if (result->aggregates_[i].IsNull()) {
              result->aggregates_[i] = input.aggregates_[i];
            } else {
              result->aggregates_[i] = result->aggregates_[i].Add(input.aggregates_[i]);
            }
            break;
          case WindowFunctionType::MinAggregate:
            if (input.aggregates_[i].IsNull()) {
              break;
            }
            if (result->aggregates_[i].IsNull()) {
              result->aggregates_[i] = input.aggregates_[i];
            } else {
              result->aggregates_[i] = result->aggregates_[i].Min(input.aggregates_[i]);
            }
            break;
          case WindowFunctionType::MaxAggregate:
            if (input.aggregates_[i].IsNull()) {
              break;
            }
            if (result->aggregates_[i].IsNull()) {
              result->aggregates_[i] = input.aggregates_[i];
            } else {
              result->aggregates_[i] = result->aggregates_[i].Max(input.aggregates_[i]);
            }
            break;
          case WindowFunctionType::Rank:
            if (input.aggregates_[i].IsNull()) {
              break;
            }
            if (result->aggregates_[i].IsNull()) {
              result->aggregates_[i] = input.aggregates_[i];
            } else {
              result->aggregates_[i] = result->aggregates_[i].Add(input.aggregates_[i]);
            }
            break;
        }
      }
    }

    /**
     * Inserts a value into the hash table and then combines it with the current aggregation.
     * @param agg_key the key to be inserted
     * @param agg_val the value to be inserted
     */
    void InsertCombine(const AggregateKey &agg_key, const AggregateValue &agg_val) {
      if (ht_.count(agg_key) == 0) {
        ht_.insert({agg_key, GenerateInitialAggregateValue()});
      }
      CombineAggregateValues(&ht_[agg_key], agg_val);
    }

    /**
     * Clear the hash table
     */
    void Clear() { ht_.clear(); }

    /** An iterator over the aggregation hash table */
    class Iterator {
     public:
      /** Creates an iterator for the aggregate map. */
      explicit Iterator(std::map<AggregateKey, AggregateValue>::const_iterator iter) : iter_{iter} {}

      /** @return The key of the iterator */
      auto Key() -> const AggregateKey & { return iter_->first; }

      /** @return The value of the iterator */
      auto Val() -> const AggregateValue & { return iter_->second; }

      /** @return The iterator before it is incremented */
      auto operator++() -> Iterator & {
        ++iter_;
        return *this;
      }

      /** @return `true` if both iterators are identical */
      auto operator==(const Iterator &other) -> bool { return this->iter_ == other.iter_; }

      /** @return `true` if both iterators are different */
      auto operator!=(const Iterator &other) -> bool { return this->iter_ != other.iter_; }

     private:
      /** Aggregates map */
      std::map<AggregateKey, AggregateValue>::const_iterator iter_;
    };

    /** @return Iterator to the start of the hash table */
    auto Begin() -> Iterator { return Iterator{ht_.cbegin()}; }

    /** @return Iterator to the end of the hash table */
    auto End() -> Iterator { return Iterator{ht_.cend()}; }

    auto IsEmpty() -> bool { return ht_.empty(); }

    void Erase(const AggregateKey &key) { ht_.erase(key); }

   private:
    /** The hash table is just a map from aggregate keys to aggregate values */
    std::map<AggregateKey, AggregateValue, Compare> ht_;
    std::unordered_map<AggregateKey, uint64_t> cnt_table_;
    /** The aggregate expressions that we have */
    std::vector<AbstractExpressionRef> agg_exprs_;
    /** The types of aggregations that we have */
    std::vector<WindowFunctionType> agg_types_;
  };

  /** @return The tuple as an AggregateKey */
  auto MakeAggregateKey(const Tuple *tuple, const std::vector<AbstractExpressionRef> &partition_by_) -> AggregateKey {
    std::vector<Value> keys;
    keys.reserve(partition_by_.size());
    for (const auto &expr : partition_by_) {
      keys.emplace_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    return {keys};
  }

  /** @return The tuple as an AggregateValue */
  auto MakeAggregateValue(const Tuple *tuple, const AbstractExpressionRef &function) -> AggregateValue {
    std::vector<Value> vals;
    vals.emplace_back(function->Evaluate(tuple, child_executor_->GetOutputSchema()));
    return {vals};
  }

  /** The window aggregation plan node to be executed */
  const WindowFunctionPlanNode *plan_;

  RID rid_{};
  Tuple tuple_{};
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  std::vector<Tuple> origin_tuples_;
  std::vector<std::vector<Tuple>> sort_tuples_;
  std::vector<AggregationHashTable> ahts_;
  // 对于非sort by语句，一次性聚合所有行，通过迭代器输出
  std::vector<AggregationHashTable::Iterator> aht_iterators_;
  uint64_t row_idx_{0};
  // 记录每个key对应的窗口大小（元素个数）
  std::vector<std::unordered_map<AggregateKey, uint64_t>> cnt_table_;
  // 记录最远的sort或partition列索引 假设列值应该与最远sort顺序一致，若全无，否则应该与原始顺序一致
  std::size_t sort_idx_{0};
  // 以1为初值，避免移除下溢问题，更优雅地处理重复值
  uint64_t cnt_duplication_{1};
};
}  // namespace bustub
