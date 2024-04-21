#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Limit) {
    const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    // Has exactly one child
    BUSTUB_ENSURE(limit_plan.children_.size() == 1, "Limit Plan should have exactly 1 children.")
    if (const auto &sort_plan = dynamic_cast<const SortPlanNode *>(limit_plan.children_[0].get());
        sort_plan != nullptr) {
      // Has exactly one child
      BUSTUB_ENSURE(sort_plan->children_.size() == 1, "Sort with multiple children?? Impossible!")
      return std::make_shared<TopNPlanNode>(limit_plan.output_schema_, sort_plan->children_[0], sort_plan->order_bys_,
                                            limit_plan.limit_);
    }
  }
  return optimized_plan;
}

}  // namespace bustub
