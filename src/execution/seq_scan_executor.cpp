//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/expressions/comparison_expression.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  txn_ = exec_ctx->GetTransaction();
  txn_manager_ = exec_ctx->GetTransactionManager();
  auto &&catalog = exec_ctx->GetCatalog();
  auto &&table_info = catalog->GetTable(plan_->table_oid_);
  table_heap_ = table_info->table_.get();
}

void SeqScanExecutor::Init() { table_iterator_ = std::make_unique<TableIterator>(table_heap_->MakeIterator()); }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_iterator_->IsEnd()) {
    return false;
  }
  auto &it = *table_iterator_;
  while (!it.IsEnd()) {
    auto &&[meta, to_tuple] = it.GetTuple();

    // 时间戳相等->当前事务未提交 或 时间戳较小->当前事务可以看见 直接发送
    // 否则 回滚版本号 直到时间戳小于当前事务读取时间戳
    // 如果有事务正在修改元组却未提交 临时时间戳ts为1000+实际时间戳，即为事务id，则直接发送
    if (meta.ts_ != txn_->GetTransactionId() && meta.ts_ > txn_->GetReadTs()) {
      std::vector<UndoLog> undo_logs;
      auto &&undo_link = txn_manager_->GetUndoLink(to_tuple.GetRid());

      // 情况1 undo1 undo2 | undo3  应该执行到undo2 事务可以看到undo3之后的记录 该元组不能跳过
      // 情况2 undo1 undo2 |        最早的时间戳大于当前事务读取时间戳 事务不可能看到 该元组跳过
      // 情况3 undo1 undo2|         应该执行到undo2 且时间戳恰好为当前事务读取时间戳 事务可以看到 该元组不能跳过
      bool is_early_break = false;
      while (undo_link.has_value() && undo_link->IsValid()) {
        auto &&undo_log = txn_manager_->GetUndoLog(*undo_link);
        if (!undo_logs.empty() && undo_log.ts_ < txn_->GetReadTs()) {
          is_early_break = true;
          break;
        }
        undo_logs.emplace_back(std::move(undo_log));
        undo_link = undo_logs.back().prev_version_;
      }

      // 最早的时间戳也大于当前事务读取时间戳，当前事务不可能看到 直接跳过
      if (!is_early_break && !undo_logs.empty() && undo_logs.back().ts_ > txn_->GetReadTs()) {
        ++it;
        continue;
      }

      auto &&target_tuple = ReconstructTuple(plan_->output_schema_.get(), to_tuple, meta, undo_logs);
      // 恢复后的元组没有被删除而最新元组被删除了
      if (target_tuple.has_value()) {
        meta.is_deleted_ = false;
        to_tuple = std::move(target_tuple.value());
      } else {
        ++it;
        continue;
      }
    }

    if (!meta.is_deleted_) {
      if (plan_->filter_predicate_ != nullptr) {
        auto &&value = plan_->filter_predicate_->Evaluate(&to_tuple, plan_->OutputSchema());
        if (value.GetTypeId() == TypeId::BOOLEAN && value.GetAs<uint8_t>() == 0) {
          ++it;
          continue;
        }
      }
      *tuple = std::move(to_tuple);
      *rid = it.GetRID();
      ++it;
      return true;
    }
    ++it;
  }
  return false;
}

}  // namespace bustub
