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
    // 获取元组元信息
    auto &&tuple_meta = table_heap->GetTupleMeta(*rid);

    // 检查写入冲突...
    // 1.如果一个未提交的旧事务对某元组进行了写操作 当新事务想要进行写操作时 应该检测到冲突 新事务不能再进行
    // 2.如果有事务对某元组做了更新并提交，那么在此之前开始的事务只能读该元组的旧数据 不能修改 否则应该检测到冲突
    // 事务不能再进行
    if ((tuple_meta.ts_ >= TXN_START_ID && tuple_meta.ts_ != txn_->GetTransactionTempTs()) ||
        (tuple_meta.ts_ < TXN_START_ID && tuple_meta.ts_ > txn_->GetReadTs())) {
      txn_->SetTainted();
      throw ExecutionException("transaction is in tainted state.");
    }

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

    tuple_meta.ts_ = txn_->GetTransactionTempTs();
    tuple_meta.is_deleted_ = true;
    table_heap->UpdateTupleMeta(tuple_meta, *rid);

    // 删除索引
    for (auto &index_info : index_info_) {
      // 单键索引
      auto &&tuple_tmp = tuple->KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_,
                                             index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(tuple_tmp, *rid, exec_ctx_->GetTransaction());
    }

    // 创建新元组
    std::vector<Value> values;
    std::vector<Value> modify_values;
    std::vector<uint32_t> attrs;
    values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    const auto &row_expr = plan_->target_expressions_;
    for (std::size_t i = 0; i < row_expr.size(); ++i) {
      if (const auto *const_value_expr = dynamic_cast<const ConstantValueExpression *>(row_expr[i].get());
          const_value_expr != nullptr) {
        undo_log.modified_fields_.emplace_back(true);
        // 取原来的值
        modify_values.emplace_back(tuple->GetValue(&child_executor_->GetOutputSchema(), i));
        attrs.emplace_back(i);
      } else {
        undo_log.modified_fields_.emplace_back(false);
      }
      values.emplace_back(row_expr[i]->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    *tuple = Tuple{values, &child_executor_->GetOutputSchema()};

    // 插入
    *rid = *table_heap->InsertTuple({txn_->GetTransactionTempTs(), false}, *tuple);

    undo_log = UndoLog{};
    undo_log.ts_ = txn_->GetReadTs();
    undo_log.is_deleted_ = true;

    // 构造undo元组
    // auto &&modify_schema = Schema::CopySchema(&child_executor_->GetOutputSchema(), attrs);
    // undo_log.tuple_ = Tuple{modify_values, &modify_schema};

    // 如果前面已经有undo节点，更新prev_version_
    //    if (auto &&undo_link = txn_manager_->GetUndoLink(*rid); undo_link.has_value()) {
    //      undo_log.prev_version_ = undo_link.value();
    //    }

    // 插入版本链条中
    cur_undo_link = txn_->AppendUndoLog(undo_log);
    if (txn_manager_->UpdateUndoLink(*rid, cur_undo_link, nullptr)) {
      // 放入事务写集中，事务commit时统一修改元组时间戳为提交时间戳
      txn_->AppendWriteSet(plan_->table_oid_, *rid);
    }

    // 新建索引
    for (auto &index_info : index_info_) {
      // 单键索引
      auto &&tuple_tmp = tuple->KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_,
                                             index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(tuple_tmp, *rid, exec_ctx_->GetTransaction());
    }

    ++updated_;
  }

  std::vector<Value> value;
  value.emplace_back(INTEGER, updated_);
  *tuple = Tuple(std::move(value), &GetOutputSchema());
  updated_ = -1;
  return true;
}

}  // namespace bustub
