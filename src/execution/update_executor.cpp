//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/executors/update_executor.h"
#include "execution/expressions/constant_value_expression.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  auto &&catalog = exec_ctx->GetCatalog();
  auto &&table_info = catalog->GetTable(plan_->table_oid_);
  table_info_ = table_info;
  index_info_ = catalog->GetTableIndexes(table_info_->name_);
  txn_ = exec_ctx->GetTransaction();
  txn_manager_ = exec_ctx->GetTransactionManager();
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (updated_ == -1) {
    return false;
  }

  auto &table_heap = table_info_->table_;
  while (child_executor_->Next(tuple, rid)) {
    ++updated_;

    // 获取元组元信息
    auto &&tuple_meta = table_heap->GetTupleMeta(*rid);

    // 删除索引
    for (auto &index_info : index_info_) {
      // 单键索引
      auto &&tuple_tmp = tuple->KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_,
                                             index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(tuple_tmp, *rid, exec_ctx_->GetTransaction());
    }

    // 1.欲更新的元组是之前的提交的事务insert的，此时meta.ts不等于事务id，需要生成且仅一个undoLog，之后就只对这个undoLog进行原地更新，不再插入新Log（因此条件2必须要有）
    // 2.欲更新的元组是当前事务insert的，此时meta.ts等于事务id，不需要生成undoLog，直接更新表堆，并且当前元组一定也没有undoLog（条件2）
    // 这里为insert生成了一个无效undolink节点，当is_insert=-1且meta.ts等于事务id，说明是当前事务插入的，直接更新表堆
    auto &&undo_link = txn_manager_->GetUndoLink(*rid);
    // update之前必然存在一个undo_link，比如insert的link，或者update的link
    assert(undo_link.has_value());
    bool is_insert = undo_link->prev_log_idx_ == -1;
    // 如果是当前事务插入的，insert时已经加入写集，这里不用再维护
    if (tuple_meta.ts_ == txn_->GetTransactionTempTs() && is_insert) {
      std::vector<Value> values;
      values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
      const auto &row_expr = plan_->target_expressions_;
      for (const auto &expr : row_expr) {
        values.emplace_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
      }
      *tuple = Tuple{values, &child_executor_->GetOutputSchema()};
      assert(table_heap->UpdateTupleInPlace(tuple_meta, *tuple, *rid));
      // 新建索引
      for (auto &index_info : index_info_) {
        // 单键索引
        auto &&tuple_tmp = tuple->KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_,
                                               index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(tuple_tmp, *rid, exec_ctx_->GetTransaction());
      }
      continue;
    }

    // 检查写入冲突...
    // 1.如果一个未提交的旧事务对某元组进行了写操作 当新事务想要进行写操作时 应该检测到冲突 新事务不能再进行
    // 2.如果有事务对某元组做了更新并提交，那么在此之前开始的事务只能读该元组的旧数据 不能修改 否则应该检测到冲突
    // 事务不能再进行
    if ((tuple_meta.ts_ >= TXN_START_ID && tuple_meta.ts_ != txn_->GetTransactionTempTs()) ||
        (tuple_meta.ts_ < TXN_START_ID && tuple_meta.ts_ > txn_->GetReadTs())) {
      txn_->SetTainted();
      throw ExecutionException("transaction is in tainted state.");
    }

    // 撤销日志
    UndoLog undo_log;
    // 创建新元组和补丁元组
    std::vector<Value> values;
    std::vector<Value> undo_values;
    std::vector<uint32_t> old_attrs;
    std::vector<uint32_t> new_attrs;

    auto &&col_count = child_executor_->GetOutputSchema().GetColumnCount();
    values.reserve(col_count);
    undo_values.reserve(col_count);
    old_attrs.reserve(col_count);
    new_attrs.reserve(col_count);
    undo_log.modified_fields_.reserve(col_count);

    for (std::size_t i = 0; i < col_count; ++i) {
      undo_log.modified_fields_.emplace_back(false);
    }

    UndoLog old_undo_log;
    // 能执行到这里的只能是update或insert之后的第一个update撤销日志
    // 前面有update的撤销日志，原地更新
    if (undo_link->prev_txn_ == txn_->GetTransactionId()) {
      old_undo_log = txn_->GetUndoLog(undo_link->prev_log_idx_);
      for (std::size_t &&i = 0; i < old_undo_log.modified_fields_.size(); ++i) {
        if (old_undo_log.modified_fields_[i]) {
          old_attrs.emplace_back(i);
        }
      }
      undo_log.modified_fields_ = old_undo_log.modified_fields_;
    }

    Schema old_schema = Schema::CopySchema(&child_executor_->GetOutputSchema(), old_attrs);

    const auto &row_expr = plan_->target_expressions_;
    for (std::size_t i = 0, j = 0; i < row_expr.size(); ++i) {
      auto &&old_value = undo_log.modified_fields_[i] ? old_undo_log.tuple_.GetValue(&old_schema, j++)
                                                      : tuple->GetValue(&child_executor_->GetOutputSchema(), i);
      auto &&new_value = row_expr[i]->Evaluate(tuple, child_executor_->GetOutputSchema());
      // 新旧值不相同或者旧的log修改了过，都需要记录覆盖
      if (!old_value.CompareExactlyEquals(new_value) || undo_log.modified_fields_[i]) {
        undo_log.modified_fields_[i] = true;
        new_attrs.emplace_back(i);
        undo_values.emplace_back(old_value);
      }
      values.emplace_back(std::move(new_value));
    }

    // 拷贝旧元组对象 注意不要拷贝指针
    auto old_tuple = *tuple;
    *tuple = Tuple{values, &child_executor_->GetOutputSchema()};

    // 新建索引
    for (auto &index_info : index_info_) {
      // 单键索引
      auto &&tuple_tmp = tuple->KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_,
                                             index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(tuple_tmp, *rid, exec_ctx_->GetTransaction());
    }

    if (IsTupleContentEqual(old_tuple, *tuple)) {
      continue;
    }

    // 更新表堆
    tuple_meta.ts_ = txn_->GetTransactionTempTs();
    assert(table_heap->UpdateTupleInPlace(tuple_meta, *tuple, *rid));
    // 如果更新列都相同，不需要更新undoLog
    if (old_attrs == new_attrs) {
      continue;
    }

    // 构造undo元组
    auto &&new_schema = Schema::CopySchema(&child_executor_->GetOutputSchema(), new_attrs);
    undo_log.tuple_ = Tuple{undo_values, &new_schema};
    undo_log.ts_ = txn_->GetReadTs();
    undo_log.is_deleted_ = false;

    // 如果前面已经有undo节点
    // 1.如果是当前事务生成的且为undoUpdate，则应该原地更新
    // 2.如果是当前事务生成的且不为undoUpdate，则必须是undoInsert，而Insert不需要日志，不可能存在。则需要生成且仅一个undoUpdate，且需要建立版本链连接
    // 3.如果不是当前事务生成的，必须生成且仅一个undoUpdate，且需要与之建立链接
    // 原地更新
    if (undo_link->prev_txn_ == txn_->GetTransactionId()) {
      auto &&prev_undo_log = txn_manager_->GetUndoLog(undo_link.value());
      assert(!prev_undo_log.modified_fields_.empty());
      // 注意版本链要接上
      undo_log.prev_version_ = prev_undo_log.prev_version_;
      txn_->ModifyUndoLog(undo_link->prev_log_idx_, undo_log);
    } else {
      // 如果为insert空版本链节点或为其他事务update的元组，生成undoLog并接上事务的版本链，之后若有update再在这个log上自我更新
      // if (undo_link->prev_log_idx_ == -1 || undo_link->prev_txn_ != txn_->GetTransactionId()) {
      undo_log.prev_version_ = undo_link.value();
      auto &&cur_undo_link = txn_->AppendUndoLog(undo_log);
      assert(txn_manager_->UpdateUndoLink(*rid, cur_undo_link, nullptr));
      // 只有事务开始且执行update才需要加入写集
      txn_->AppendWriteSet(plan_->table_oid_, *rid);
    }
  }

  std::vector<Value> value;
  value.emplace_back(INTEGER, updated_);
  *tuple = Tuple(std::move(value), &GetOutputSchema());
  updated_ = -1;
  return true;
}

}  // namespace bustub
