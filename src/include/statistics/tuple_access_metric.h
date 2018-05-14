//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// database_metric.h
//
// Identification: src/include/statistics/tuple_access_metric.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include <unordered_map>
#include <unordered_set>
#include "statistics/abstract_metric.h"
#include "common/internal_types.h"
#include "concurrency/transaction_context.h"

namespace peloton {
namespace stats {

// TODO(tianyu): This is a hack to not log internal transactions. Fix this eventually
class TupleAccessRawData: public AbstractRawData {
 public:

  static std::atomic<unsigned long long> num_reads;

  inline void LogTupleRead(txn_id_t tid) {
    static unsigned long long threshold = 100000;

    num_reads.fetch_add(1);

    unsigned long long cur_num_reads = num_reads.load(std::memory_order_acquire);
    
    if (cur_num_reads % threshold == 0) {
      // num_reads.store(0,std::memory_order_acquire);
    }

    // LOG_INFO("CALLED ME READ");
    if (begins_.find(tid) != begins_.end()) {
      // increase the static variable
      // if that > threshold, then print it onto the console
      tuple_access_counters_[tid]++;
    }
  }
  inline void LogCommit(txn_id_t tid) {
    // LOG_INFO("CALLED ME COMMIT");
    if (begins_.find(tid) != begins_.end())
      commits_.insert(tid);
  }

  inline void LogAbort(txn_id_t tid) {
    // LOG_INFO("CALLED ME ABORT");
    if (begins_.find(tid) != begins_.end())
      aborts_.insert(tid);
  }

  inline void LogTxnBegin(txn_id_t tid) {
    // LOG_INFO("CALLED ME BEGIN");
    begins_.insert(tid);
  }

  inline void Aggregate(AbstractRawData &other) override {
    auto &other_db_metric = dynamic_cast<TupleAccessRawData &>(other);
    for (auto &entry : other_db_metric.tuple_access_counters_)
      tuple_access_counters_[entry.first] += entry.second;
    for (auto &txn : other_db_metric.commits_)
      commits_.insert(txn);
    for (auto &txn : other_db_metric.aborts_)
      aborts_.insert(txn);
  }

  void UpdateAndPersist() override;

  // TODO(Tianyu): Pretty Print
  const std::string GetInfo() const override { return "TupleAccessRawData"; };
 private:
  void WriteToCatalog(txn_id_t tid, bool complete, bool commit, concurrency::TransactionContext *txn);
  std::unordered_map<txn_id_t, uint64_t> tuple_access_counters_;
  std::unordered_set<txn_id_t> begins_, commits_, aborts_;
};

class TupleAccessMetric : public AbstractMetric<TupleAccessRawData> {
 public:
  void OnTransactionBegin(const concurrency::TransactionContext *context) override {
    GetRawData()->LogTxnBegin(context->GetTransactionId());
  }
  void OnTransactionCommit(const concurrency::TransactionContext *context,
                           oid_t) override {
    GetRawData()->LogCommit(context->GetTransactionId());
  }

  void OnTransactionAbort(const concurrency::TransactionContext *context,
                          oid_t) override {
    GetRawData()->LogAbort(context->GetTransactionId());
  }

  void OnTupleRead(const concurrency::TransactionContext *context,
                   oid_t) override {
    GetRawData()->LogTupleRead(context->GetTransactionId());
  }
};
} // namespace stats
} // namespace peloton