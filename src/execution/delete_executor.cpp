//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  auto &&catalog = exec_ctx->GetCatalog();
  auto &&table_info = catalog->GetTable(plan_->table_oid_);
  table_heap_ = table_info->table_.get();
  index_info_ = catalog->GetTableIndexes(table_info->name_);
  txn_ = exec_ctx->GetTransaction();
  txn_manager_ = exec_ctx->GetTransactionManager();
}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (deleted_ == -1) {
    return false;
  }

  while (child_executor_->Next(tuple, rid)) {
    // 获取元组元信息
    auto &&tuple_meta = table_heap_->GetTupleMeta(*rid);

    // 检查写入冲突...
    // 1.如果一个未提交的旧事务对某元组进行了写操作 当新事务想要进行写操作时 应该检测到冲突 新事务不能再进行
    // 2.如果有事务对某元组做了更新并提交，那么在此之前开始的事务只能读该元组的旧数据 不能修改 否则应该检测到冲突
    // 事务不能再进行
    if ((tuple_meta.ts_ >= TXN_START_ID && tuple_meta.ts_ != txn_->GetTransactionTempTs()) ||
        (tuple_meta.ts_ < TXN_START_ID && tuple_meta.ts_ > txn_->GetReadTs())) {
      txn_->SetTainted();
      throw ExecutionException("transaction is in tainted state.");
    }

    // undo删除 因为是逻辑删除只需要改标志位
    UndoLog undo_log;
    undo_log.ts_ = txn_->GetReadTs();
    undo_log.is_deleted_ = false;

    // 如果前面已经有undo节点，更新prev_version_
    if (auto &&undo_link = txn_manager_->GetUndoLink(*rid); undo_link.has_value()) {
      undo_log.prev_version_ = undo_link.value();
    }

    // 得到undo链表节点并更新作为新的版本链表头
    UndoLink cur_undo_link = txn_->AppendUndoLog(undo_log);
    if (txn_manager_->UpdateUndoLink(*rid, cur_undo_link, nullptr)) {
      // 放入事务写集中，事务commit时统一修改元组时间戳为提交时间戳
      txn_->AppendWriteSet(plan_->table_oid_, *rid);
    }

    // 逻辑删除
    tuple_meta.ts_ = txn_->GetTransactionTempTs();
    tuple_meta.is_deleted_ = true;
    table_heap_->UpdateTupleMeta(tuple_meta, *rid);

    // 删除索引
    for (auto &index_info : index_info_) {
      // 单键索引
      auto &&tuple_tmp = tuple->KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_,
                                             index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(tuple_tmp, *rid, exec_ctx_->GetTransaction());
    }

    ++deleted_;
  }

  std::vector<Value> value;
  value.emplace_back(INTEGER, deleted_);
  *tuple = Tuple(std::move(value), &GetOutputSchema());
  deleted_ = -1;
  return true;
}

}  // namespace bustub
