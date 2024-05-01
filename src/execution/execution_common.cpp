#include "execution/execution_common.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  // 设计的想法：unlog中直接存储对tuple的快照，这样就可以直接赋值
  // 为了省出空间，目前的设计tuple只存储了部分，这样只能采取逐级回滚的方法
  auto target_meta = base_meta;

  std::vector<Value> values;
  std::vector<uint32_t> attrs;

  for (std::size_t i = 0; i < schema->GetColumnCount(); ++i) {
    values.emplace_back(base_tuple.GetValue(schema, i));
  }

  for (const auto &undo_log : undo_logs) {
    target_meta.ts_ = undo_log.ts_;
    if (undo_log.is_deleted_) {
      target_meta.is_deleted_ = true;
    } else {
      attrs.clear();
      target_meta.is_deleted_ = false;
      for (std::size_t i = 0; i < undo_log.modified_fields_.size(); ++i) {
        if (undo_log.modified_fields_[i]) {
          attrs.emplace_back(i);
        }
      }
      auto &&key_schema = Schema::CopySchema(schema, attrs);
      for (std::size_t i = 0, j = 0; i < undo_log.modified_fields_.size(); ++i) {
        if (undo_log.modified_fields_[i]) {
          values[i] = undo_log.tuple_.GetValue(&key_schema, j++);
        }
      }
    }
  }

  return target_meta.is_deleted_ ? std::nullopt : std::optional<Tuple>({values, schema});
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  fmt::println(
      stderr,
      "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
      "finished task 2. Implementing this helper function will save you a lot of time for debugging in later tasks.");

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

}  // namespace bustub
