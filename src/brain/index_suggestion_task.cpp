//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_suggestion_task.cpp
//
// Identification: src/brain/index_suggestion_task.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <include/brain/index_selection_util.h>
#include "include/brain/index_suggestion_task.h"
#include "catalog/query_history_catalog.h"
#include "brain/index_selection.h"

namespace peloton {
namespace brain {

// Interval in seconds.
struct timeval IndexSuggestionTask::interval {
  10, 0
};

uint64_t IndexSuggestionTask::last_timestamp = 0;

uint64_t IndexSuggestionTask::tuning_threshold = 60;

size_t IndexSuggestionTask::max_index_cols = 3;

size_t IndexSuggestionTask::enumeration_threshold = 2;

size_t IndexSuggestionTask::num_indexes = 10;

void IndexSuggestionTask::Task(BrainEnvironment *env) {
  (void)env;
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  LOG_INFO("Started Index Suggestion Task");

  // Query the catalog for new queries.
  auto query_catalog = &catalog::QueryHistoryCatalog::GetInstance(txn);
  auto query_history =
      query_catalog->GetQueryStringsAfterTimestamp(last_timestamp, txn);
  if (query_history->size() > tuning_threshold) {
    LOG_INFO("Tuning threshold has crossed. Time to tune the DB!");

    // Run the index selection.
    std::vector<std::string> queries;
    for (auto query_pair: *query_history) {
      queries.push_back(query_pair.second);
    }

    // TODO: Handle multiple databases
    brain::Workload workload(queries, DEFAULT_DB_NAME);
    brain::IndexSelection is = {workload, max_index_cols, enumeration_threshold,
                                num_indexes};
    brain::IndexConfiguration best_config;
    is.GetBestIndexes(best_config);

    for (auto index: best_config.GetIndexes()) {
      // Create RPC for index creation on the server side.
      CreateIndexRPC(index.get());
    }

    // Update the last_timestamp to the be the latest query's timestamp in
    // the current workload, so that we fetch the new queries next time.
    // TODO[vamshi]: Make this efficient. Currently assuming that the latest
    // query can be anywhere in the vector. if the latest query is always at the
    // end, then we can avoid scan over all the queries.
    last_timestamp = GetLatestQueryTimestamp(query_history.get());
  } else {
    LOG_INFO("Tuning - not this time");
  }
  txn_manager.CommitTransaction(txn);
}

void IndexSuggestionTask::CreateIndexRPC(brain::HypotheticalIndexObject *index) {
  // TODO: Remove hardcoded database name and server end point.
  capnp::EzRpcClient client("localhost:15445");
  PelotonService::Client peloton_service = client.getMain<PelotonService>();

  auto request = peloton_service.createIndexRequest();
  request.getRequest().setDatabaseOid(index->db_oid);
  request.getRequest().setTableOid(index->table_oid);
  request.getRequest().setUniqueKeys(false);

  auto col_list = request.getRequest().initKeyAttrOids(index->column_oids.size());
  for (auto i=0UL; i<index->column_oids.size(); i++) {
    col_list.set(i, index->column_oids[i]);
  }

  PELOTON_ASSERT(index->column_oids.size() > 0);
  auto response = request.send().wait(client.getWaitScope());
}

uint64_t IndexSuggestionTask::GetLatestQueryTimestamp(
    std::vector<std::pair<uint64_t, std::string>> *queries) {
  uint64_t latest_time = 0;
  for (auto query : *queries) {
    if (query.first > latest_time) {
      latest_time = query.first;
    }
  }
  return latest_time;
}
}
}