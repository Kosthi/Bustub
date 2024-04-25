#include "execution/executors/window_function_executor.h"
#include "execution/executors/seq_scan_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

// 假设每个窗口函数的partition by都一致，则通过mysql知，都是根据partition by大小升序排列输出
WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  origin_tuples_.clear();
  sort_tuples_.clear();
  ahts_.clear();
  aht_iterators_.clear();
  child_executor_->Init();
  row_idx_ = 0;
  cnt_table_.clear();
  sort_idx_ = 0;

  while (child_executor_->Next(&tuple_, &rid_)) {
    origin_tuples_.emplace_back(tuple_);
  }

  for (std::size_t i = 0; i < plan_->columns_.size(); ++i) {
    if (const auto *col_expr = dynamic_cast<const ColumnValueExpression *>(plan_->columns_[i].get());
        col_expr != nullptr) {
      // 行值表达式
      if (col_expr->GetColIdx() != UINT32_MAX) {
        sort_tuples_.emplace_back(origin_tuples_);
        ahts_.emplace_back(std::vector<AbstractExpressionRef>{}, std::vector<WindowFunctionType>{});
        cnt_table_.emplace_back();
      } else {
        auto &&window_function = plan_->window_functions_.at(i);
        ahts_.emplace_back(std::vector<AbstractExpressionRef>{window_function.function_},
                           std::vector<WindowFunctionType>{window_function.type_});
        if (!window_function.order_by_.empty()) {
          sort_idx_ = i;
        } else {
          if (!window_function.partition_by_.empty()) {
            sort_idx_ = i;
          }
          // 没有sort 照原始顺序聚合但partition要升序
          for (auto &tuple : origin_tuples_) {
            ahts_.back().InsertCombine(MakeAggregateKey(&tuple, window_function.partition_by_),
                                       MakeAggregateValue(&tuple, window_function.function_));
          }
        }

        // 无论是part还是sort都要sort
        // C++20 support structured bindings.
        sort_tuples_.emplace_back(origin_tuples_);
        if (!window_function.order_by_.empty() || !window_function.partition_by_.empty()) {
          std::sort(sort_tuples_.back().begin(), sort_tuples_.back().end(), [&](const Tuple &a, const Tuple &b) {
            // 如果有partition则要在窗口内排序
            if (!window_function.partition_by_.empty()) {
              for (auto &expr : window_function.partition_by_) {
                auto &&lhs = expr->Evaluate(&a, *plan_->output_schema_);
                auto &&rhs = expr->Evaluate(&b, *plan_->output_schema_);
                if (lhs.CompareEquals(rhs) != CmpBool::CmpTrue) {
                  return lhs.CompareLessThan(rhs) == CmpBool::CmpTrue;
                }
              }
            }

            for (auto &[order_type, expr] : window_function.order_by_) {
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

        cnt_table_.emplace_back();
        for (auto &tuple : origin_tuples_) {
          auto &&itt = cnt_table_.back().find(MakeAggregateKey(&tuple, window_function.partition_by_));
          if (itt != cnt_table_.back().end()) {
            ++itt->second;
          } else {
            cnt_table_.back().emplace(MakeAggregateKey(&tuple, window_function.partition_by_), 1);
          }
        }
        // wrong use
        // aht_iterators_.emplace_back(ahts_.back().Begin());
      }
    } else {
      BUSTUB_ASSERT(false, "Error Expression Type.");
    }
  }

  for (auto &aht : ahts_) {
    aht_iterators_.emplace_back(aht.Begin());
  }
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (row_idx_ == origin_tuples_.size()) {
    return false;
  }

  std::vector<Value> values;
  values.reserve(plan_->columns_.size());

  for (std::size_t i = 0; i < plan_->columns_.size(); ++i) {
    auto &&expr = plan_->columns_[i];
    if (const auto *col_expr = dynamic_cast<const ColumnValueExpression *>(expr.get()); expr != nullptr) {
      // 列值表达式
      if (col_expr->GetColIdx() != UINT32_MAX) {
        *tuple = sort_tuples_[sort_idx_][row_idx_];
        values.emplace_back(col_expr->Evaluate(tuple, plan_->OutputSchema()));
        continue;
      }
      auto &&window_function = plan_->window_functions_.at(i);
      // 聚合表达式
      if (window_function.order_by_.empty()) {
        // 处理所有行的聚合
        // 实际上就一个聚合函数
        for (auto &v : aht_iterators_[i].Val().aggregates_) {
          values.emplace_back(v);
        }
        if (!aht_iterators_[i].Key().group_bys_.empty()) {
          if (--cnt_table_[i][aht_iterators_[i].Key()] == 0) {
            ++aht_iterators_[i];
          }
        }
      } else {
        auto &&aggregate_key =
            MakeAggregateKey(&sort_tuples_[i][row_idx_], plan_->window_functions_.at(i).partition_by_);
        auto &&aggregate_value =
            MakeAggregateValue(&sort_tuples_[i][row_idx_], plan_->window_functions_.at(i).function_);
        // 对于rank()特殊处理
        if (plan_->window_functions_.at(i).type_ == WindowFunctionType::Rank && row_idx_ > 0) {
          auto &&prev = sort_tuples_[i][row_idx_ - 1].GetValue(plan_->output_schema_.get(), 0);
          auto &&curr = sort_tuples_[i][row_idx_].GetValue(plan_->output_schema_.get(), 0);
          if (prev.CompareEquals(curr) != CmpBool::CmpTrue) {
            while ((cnt_duplication_--) != 0U) {
              ahts_[i].InsertCombine(aggregate_key, aggregate_value);
            }
            cnt_duplication_ = 1;
          } else {
            ++cnt_duplication_;
          }
        } else {
          // 处理第一个到当前行的聚合
          ahts_[i].InsertCombine(aggregate_key, aggregate_value);
        }
        // 插入新数据以后原来的迭代器会失效！！！
        auto &&it = ahts_[i].Begin();
        for (auto &v : it.Val().aggregates_) {
          values.emplace_back(v);
        }
        if (--cnt_table_[i][it.Key()] == 0) {
          ahts_[i].Erase(it.Key());
        }
      }
    }
  }

  ++row_idx_;
  *tuple = Tuple(values, plan_->output_schema_.get());
  return true;
}
}  // namespace bustub
