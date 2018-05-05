//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// backend_stats_context.h
//
// Identification: src/statistics/backend_stats_context.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <map>
#include <sstream>
#include <thread>
#include <unordered_map>

#include "common/container/cuckoo_map.h"
#include "common/container/lock_free_queue.h"
#include "common/platform.h"
#include "common/synchronization/spin_latch.h"
#include "statistics/database_metric.h"
#include "statistics/index_metric.h"
#include "statistics/latency_metric.h"
#include "statistics/oid_aggr_reducer.h"
#include "statistics/query_metric.h"
#include "statistics/stats_channel.h"
#include "statistics/table_metric.h"

#define QUERY_METRIC_QUEUE_SIZE 100000
#define TILE_GROUP_CHANNEL_SIZE 10000

namespace peloton {

class Statement;

namespace index {
class IndexMetadata;
}  // namespace index

namespace stats {

class CounterMetric;

/**
 * Context of backend stats as a singleton per thread
 */
class BackendStatsContext {
 public:
  static BackendStatsContext *GetInstance();

  BackendStatsContext(size_t max_latency_history, bool regiser_to_aggregator);
  ~BackendStatsContext();

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  inline std::thread::id GetThreadId() { return thread_id_; }

  // Returns the table metric with the given database ID and table ID
  TableMetricOld *GetTableMetric(oid_t database_id, oid_t table_id);

  // Returns the database metric with the given database ID
  DatabaseMetricOld *GetDatabaseMetric(oid_t database_id);

  // Returns the index metric with the given database ID, table ID, and
  // index ID
  IndexMetricOld *GetIndexMetric(oid_t database_id, oid_t table_id,
                                 oid_t index_id);

  // Returns the metrics for completed queries
  LockFreeQueue<std::shared_ptr<QueryMetric>> &GetCompletedQueryMetrics() {
    return completed_query_metrics_;
  };

  // Returns the metric for the on going query
  QueryMetric *GetOnGoingQueryMetric() { return ongoing_query_metric_.get(); }

  // Returns the latency metric
  LatencyMetricOld &GetQueryLatencyMetric();

  // Increment the read stat for given tile group
  void IncrementTableReads(oid_t tile_group_id);

  // Increment the insert stat for given tile group
  void IncrementTableInserts(oid_t tile_group_id);

  // Increment the update stat for given tile group
  void IncrementTableUpdates(oid_t tile_group_id);

  // Increment the delete stat for given tile group
  void IncrementTableDeletes(oid_t tile_group_id);

  // Increment the read stat for given index by read_count
  void IncrementIndexReads(size_t read_count, index::IndexMetadata *metadata);

  // Increment the insert stat for index
  void IncrementIndexInserts(index::IndexMetadata *metadata);

  // Increment the update stat for index
  void IncrementIndexUpdates(index::IndexMetadata *metadata);

  // Increment the delete stat for index
  void IncrementIndexDeletes(size_t delete_count,
                             index::IndexMetadata *metadata);

  // Increment the commit stat for given database
  void IncrementTxnCommitted(oid_t database_id);

  // Increment the abortion stat for given database
  void IncrementTxnAborted(oid_t database_id);

  /**
   * @brief Increase the memory allocation stats of a given table
   *
   * @param database_id database id of the given table
   * @param table_id table id of the given table
   * @param bytes bytes that is allocated to the table
   */
  void IncreaseTableMemoryAlloc(oid_t database_id, oid_t table_id,
                                int64_t bytes);

  /**
   * @brief Increase the memory usage stats of a given table
   *
   * @param database_id database id of the given table
   * @param table_id table id of the given table
   * @param bytes bytes that is used of the table
   */
  void IncreaseTableMemoryUsage(oid_t database_id, oid_t table_id,
                                int64_t bytes);

  /**
   * @brief Decrease the memory allocation stats of a given table
   *
   * @param database_id database id of the given table
   * @param table_id table id of the given table
   * @param bytes bytes that is dealloc of the table
   */
  void DecreaseTableMemoryAlloc(oid_t database_id, oid_t table_id,
                                int64_t bytes);

  /**
   * @brief Decrease the memory usage stats of a given table
   *
   * @param database_id database id of the given table
   * @param table_id table id of the given table
   * @param bytes bytes that becomes available of the table
   */
  void DecreaseTableMemoryUsage(oid_t database_id, oid_t table_id,
                                int64_t bytes);

  void AddTileGroup(oid_t tile_group);

  StatsChannel<oid_t, OidAggrReducer> &GetTileGroupChannel();

  // Initialize the query stat
  void InitQueryMetric(const std::shared_ptr<Statement> statement,
                       const std::shared_ptr<QueryMetric::QueryParams> params);

  //===--------------------------------------------------------------------===//
  // HELPER FUNCTIONS
  //===--------------------------------------------------------------------===//

  /**
   * Aggregate another BackendStatsContext to myself
   */
  void Aggregate(BackendStatsContext &source);

  // Resets all metrics (and sub-metrics) to their starting state
  // (e.g., sets all counters to zero)
  void Reset();

  std::string ToString() const;

  // Returns the total number of query aggregated so far
  oid_t GetQueryCount() { return aggregated_query_count_; }

  // Resets the total number of query aggregated to zero
  void ResetQueryCount() { aggregated_query_count_ = 0; }

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // Database metrics
  std::unordered_map<oid_t, std::unique_ptr<DatabaseMetricOld>>
      database_metrics_{};

  // Table metrics
  std::unordered_map<oid_t, std::unique_ptr<TableMetricOld>> table_metrics_{};

  // Index metrics
  CuckooMap<oid_t, std::shared_ptr<IndexMetricOld>> index_metrics_{};

  // Index oids
  std::unordered_set<oid_t> index_ids_;

  // Metrics for completed queries
  LockFreeQueue<std::shared_ptr<QueryMetric>> completed_query_metrics_{
      QUERY_METRIC_QUEUE_SIZE};

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // The query metric for the on going metric
  std::shared_ptr<QueryMetric> ongoing_query_metric_ = nullptr;

  // The thread ID of this worker
  std::thread::id thread_id_;

  // Latencies recorded by this worker
  LatencyMetricOld txn_latencies_;

  // Whether this context is registered to the global aggregator
  bool is_registered_to_aggregator_;

  // The total number of queries aggregated
  oid_t aggregated_query_count_ = 0;

  // Index oid spin lock
  common::synchronization::SpinLatch index_id_lock;

  // Channel collecting oid of newly created TileGroups
  StatsChannel<oid_t, OidAggrReducer> tile_group_channel_{
      TILE_GROUP_CHANNEL_SIZE};

  //===--------------------------------------------------------------------===//
  // HELPER FUNCTIONS
  //===--------------------------------------------------------------------===//

  // Mark the on going query as completed and move it to completed query queue
  void CompleteQueryMetric();

  // Get the mapping table of backend stat context for each thread
  static CuckooMap<std::thread::id, std::shared_ptr<BackendStatsContext>> &
  GetBackendContextMap(void);
};

}  // namespace stats
}  // namespace peloton
