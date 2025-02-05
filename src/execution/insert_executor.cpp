//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  auto &&catalog = exec_ctx->GetCatalog();
  auto &&table_info = catalog->GetTable(plan_->table_oid_);
  table_heap_ = table_info->table_.get();
  index_info_ = catalog->GetTableIndexes(table_info->name_);
  txn_ = exec_ctx->GetTransaction();
  txn_manager_ = exec_ctx->GetTransactionManager();
}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (inserted_ == -1) {
    return false;
  }

  while (child_executor_->Next(tuple, rid)) {
    // MVCC 临时时间戳并放入写集
    *rid = *table_heap_->InsertTuple({txn_->GetTransactionTempTs(), false}, *tuple);

    // 1.1
    // 如果是当前事务首次插入元组，不生成undoLog，扫描时因为不存在undoLog而跳过该元组，实现了与meta.delete设置为true（undoLog）一致的效果
    // 1.2 如果当前事务提交了，之前事务看不到，之后的事务能看到，相同读取时间戳的事务需要undo，结果log为空而跳过
    // 不需要生成undoLog，但是需要生成一个undoLink节点，表示有undoInsert的信息，特判log_idx=-1来找到
    // UndoLog undo_log;
    // undo_log.ts_ = txn_->GetReadTs();
    // undo_log.is_deleted_ = true;
    // 用log_idx=-1来标记是否是插入的撤销日志，此时txn为读取时间戳+1
    UndoLink undo_link = {txn_->GetReadTs() + 1, -1};
    txn_manager_->UpdateUndoLink(*rid, undo_link, nullptr);

    txn_->AppendWriteSet(plan_->table_oid_, *rid);

    for (auto &index_info : index_info_) {
      // 单键索引
      auto &&tuple_tmp = tuple->KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_,
                                             index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(tuple_tmp, *rid, exec_ctx_->GetTransaction());
    }

    ++inserted_;
  }

  std::vector<Value> value;
  value.emplace_back(INTEGER, inserted_);
  *tuple = Tuple(std::move(value), &GetOutputSchema());
  inserted_ = -1;
  return true;
}

}  // namespace bustub
