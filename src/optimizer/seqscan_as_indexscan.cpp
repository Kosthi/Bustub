#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    try {
      const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
      if (seq_scan_plan.filter_predicate_ == nullptr) {
        return optimized_plan;
      }
      const auto &comparison_expression = dynamic_cast<const ComparisonExpression &>(*seq_scan_plan.filter_predicate_);
      if (comparison_expression.comp_type_ == ComparisonType::Equal) {
        auto &lcol = comparison_expression.GetChildAt(0);
        auto &column_value = dynamic_cast<const ColumnValueExpression &>(*lcol);
        // 得到右值
        auto &rval = comparison_expression.GetChildAt(1);
        auto &&pred_key =
            std::make_shared<ConstantValueExpression>(*dynamic_cast<ConstantValueExpression *>(rval.get()));
        // 得到key schema
        std::vector<uint32_t> attrs{column_value.GetColIdx()};
        auto &&key_schema = std::make_shared<Schema>(Schema::CopySchema(seq_scan_plan.output_schema_.get(), attrs));
        // 得到索引oid
        auto &&index_meta = MatchIndex(seq_scan_plan.table_name_, column_value.GetColIdx());
        if (index_meta == std::nullopt) {
          return optimized_plan;
        }
        auto &&index_oid = std::get<0>(index_meta.value());
        return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, seq_scan_plan.table_oid_, index_oid,
                                                   seq_scan_plan.filter_predicate_, std::move(pred_key),
                                                   std::move(key_schema));
      }
    } catch (const std::bad_cast &e) {
      return optimized_plan;
    }
  }
  return optimized_plan;
}

}  // namespace bustub
