#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

void Dfs(const AbstractExpression *root, std::vector<AbstractExpressionRef> &left_tuples,
         std::vector<AbstractExpressionRef> &right_tuples) {
  if (const auto *expr = dynamic_cast<const ComparisonExpression *>(root); expr != nullptr) {
    if (expr->comp_type_ == ComparisonType::Equal) {
      if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
          left_expr != nullptr) {
        if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
            right_expr != nullptr) {
          // Ensure both exprs have tuple_id == 0
          auto left_expr_tuple_0 =
              std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType());
          auto right_expr_tuple_0 =
              std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType());
          // Now it's in form of <column_expr> = <column_expr>.

          // Ensure right child is table scan
          // const auto &right_seq_scan = dynamic_cast<const SeqScanPlanNode &>(*nlj_plan.GetRightPlan());
          if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
            left_tuples.emplace_back(left_expr_tuple_0);
            right_tuples.emplace_back(right_expr_tuple_0);
          }
          if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
            left_tuples.emplace_back(right_expr_tuple_0);
            right_tuples.emplace_back(left_expr_tuple_0);
          }
        }
      }
    }
  } else if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(root); logic_expr != nullptr) {
    if (logic_expr->logic_type_ == LogicType::And) {
      BUSTUB_ENSURE(logic_expr->children_.size() == 2, "LogicExpression should have exactly 2 children.")
      Dfs(logic_expr->children_[0].get(), left_tuples, right_tuples);
      Dfs(logic_expr->children_[1].get(), left_tuples, right_tuples);
    }
  }
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly two children
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.")
    std::vector<AbstractExpressionRef> left_tuples;
    std::vector<AbstractExpressionRef> right_tuples;
    Dfs(nlj_plan.Predicate().get(), left_tuples, right_tuples);
    if (left_tuples.empty() && right_tuples.empty()) {
      return optimized_plan;
    }
    return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
                                              left_tuples, right_tuples, nlj_plan.GetJoinType());
  }
  return optimized_plan;
}

}  // namespace bustub
