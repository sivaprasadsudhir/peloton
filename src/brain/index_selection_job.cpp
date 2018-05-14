//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_selection_job.cpp
//
// Identification: src/brain/index_selection_job.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <include/optimizer/optimizer.h>
#include "brain/index_selection_util.h"
#include "brain/index_selection_job.h"
#include "brain/index_selection.h"
#include "catalog/query_history_catalog.h"
#include "catalog/system_catalogs.h"
#include "optimizer/stats/stats_storage.h"

namespace peloton {
namespace brain {

#define BRAIN_SUGGESTED_INDEX_MAGIC_STR "brain_suggested_index"

bool IndexSelectionJob::is_running = false;

void IndexSelectionJob::OnJobInvocation(BrainEnvironment *env) {

  if (!is_running) {
    return;
  }

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  LOG_INFO("Started Index Suggestion Task");
  optimizer::Optimizer::optimizer_calls = 0;

  // Query the catalog for new SQL queries.
  // New SQL queries are the queries that were added to the system
  // after the last_timestamp_
  auto &query_catalog = catalog::QueryHistoryCatalog::GetInstance(txn);
  auto query_history =
      query_catalog.GetQueryStringsAfterTimestamp(last_timestamp_, txn);
  if (query_history->size() > num_queries_threshold_) {
    LOG_INFO("Tuning threshold has crossed. Time to tune the DB!");

    // Run the index selection.
    std::vector<std::string> queries;
    for (auto query_pair : *query_history) {
      queries.push_back(query_pair.second);
    }

    // TODO: Handle multiple databases
    brain::Workload workload(queries, DEFAULT_DB_NAME, txn);
    LOG_INFO("Knob Num Indexes: %zu", env->GetIndexSelectionKnobs().num_indexes_);
    LOG_INFO("Knob Naive: %zu", env->GetIndexSelectionKnobs().naive_enumeration_threshold_);
    LOG_INFO("Knob Num Iterations: %zu", env->GetIndexSelectionKnobs().num_iterations_);


    size_t max_index_cols = 3;
    size_t enumeration_threshold = 2;
    size_t num_indexes = 10;
    brain::IndexSelectionKnobs knobs = {max_index_cols, enumeration_threshold,
                                        num_indexes};
    brain::IndexSelection is = {workload, knobs, txn};
    brain::IndexConfiguration best_config;
    Timer<std::ratio<1, 1000>> timer_ms_;

    timer_ms_.Start();
    is.GetBestIndexes(best_config);
    timer_ms_.Stop();

    LOG_INFO("Naive enumeration : 2 : %lf", timer_ms_.GetDuration());

    max_index_cols = 3;
    enumeration_threshold = 3;
    num_indexes = 10;
    knobs = {max_index_cols, enumeration_threshold, num_indexes};
    is = {workload, knobs, txn};

    timer_ms_.Reset();
    timer_ms_.Start();
    is.GetBestIndexes(best_config);
    timer_ms_.Stop();

    LOG_INFO("Naive enumeration : 3 : %lf", timer_ms_.GetDuration());

    max_index_cols = 3;
    enumeration_threshold = 3;
    num_indexes = 10;
    knobs = {max_index_cols, enumeration_threshold, num_indexes};
    is = {workload, knobs, txn};

    timer_ms_.Reset();
    timer_ms_.Start();
    is.GetBestIndexes(best_config);
    timer_ms_.Stop();

    LOG_INFO("Naive enumeration : 4 : %lf", timer_ms_.GetDuration());

    // brain::IndexSelection is = {workload, env->GetIndexSelectionKnobs(), txn};
    // LOG_INFO("Workload created");

    // if (best_config.IsEmpty()) {
    //   LOG_INFO("Best config is empty");
    // }

    // // Get the existing indexes and drop them.
    // // TODO: Handle multiple databases
    // auto database_object = catalog::Catalog::GetInstance()->GetDatabaseObject(
    //   DEFAULT_DB_NAME, txn);
    // auto pg_index = catalog::Catalog::GetInstance()
    //   ->GetSystemCatalogs(database_object->GetDatabaseOid())
    //   ->GetIndexCatalog();
    // auto indexes = pg_index->GetIndexObjects(txn);
    // for (auto index : indexes) {
    //   auto index_name = index.second->GetIndexName();
    //   // TODO [vamshi]: REMOVE THIS IN THE FINAL CODE
    //   // This is a hack for now. Add a boolean to the index catalog to
    //   // find out if an index is a brain suggested index/user created index.
    //   if (index_name.find(BRAIN_SUGGESTED_INDEX_MAGIC_STR) !=
    //       std::string::npos) {
    //     bool found = false;
    //     for (auto installed_index: best_config.GetIndexes()) {
    //       if ((index.second.get()->GetTableOid() == installed_index.get()->table_oid) &&
    //       (index.second.get()->GetKeyAttrs() == installed_index.get()->column_oids)) {
    //         found = true;
    //       }
    //     }
    //     // Drop only indexes which are not suggested this time.
    //     if (!found) {
    //       LOG_DEBUG("Dropping Index: %s", index_name.c_str());
    //       DropIndexRPC(env, database_object->GetDatabaseOid(), index.second.get());
    //     }
    //   }
    // }

    // for (auto index : best_config.GetIndexes()) {
    //   // Create RPC for index creation on the server side.
    //   CreateIndexRPC(env, index.get());
    // }

    // // Update the last_timestamp to the be the latest query's timestamp in
    // // the current workload, so that we fetch the new queries next time.
    // // TODO[vamshi]: Make this efficient. Currently assuming that the latest
    // // query can be anywhere in the vector. if the latest query is always at the
    // // end, then we can avoid scan over all the queries.
    // last_timestamp_ = GetLatestQueryTimestamp(query_history.get());
  } else {
    LOG_INFO("Tuning - not this time");
  }
  txn_manager.CommitTransaction(txn);

  LOG_INFO("NUMBER OF OPT CALLS MADE BY INDEX SELECTION: %llu", optimizer::Optimizer::optimizer_calls.load());
  optimizer::Optimizer::optimizer_calls = 0;

  is_running = false;
}

void IndexSelectionJob::CreateIndexRPC(BrainEnvironment *env, brain::HypotheticalIndexObject *index) {
  // TODO: Remove hardcoded database name and server end point.
  auto &client = env->GetPelotonClient();
  PelotonService::Client peloton_service = client.getMain<PelotonService>();

  // Create the index name: concat - db_id, table_id, col_ids
  std::stringstream sstream;
  sstream << BRAIN_SUGGESTED_INDEX_MAGIC_STR << "_" << index->db_oid << "_"
          << index->table_oid << "_";
  std::vector<oid_t> col_oid_vector;
  for (auto col : index->column_oids) {
    col_oid_vector.push_back(col);
    sstream << col << "_";
  }
  auto index_name = sstream.str();

  auto request = peloton_service.createIndexRequest();
  request.getRequest().setDatabaseOid(index->db_oid);
  request.getRequest().setTableOid(index->table_oid);
  request.getRequest().setIndexName(index_name);
  request.getRequest().setUniqueKeys(false);

  auto col_list =
      request.getRequest().initKeyAttrOids(index->column_oids.size());
  for (auto i = 0UL; i < index->column_oids.size(); i++) {
    col_list.set(i, index->column_oids[i]);
  }

  PELOTON_ASSERT(index->column_oids.size() > 0);
  auto response = request.send().wait(client.getWaitScope());
}

void IndexSelectionJob::DropIndexRPC(BrainEnvironment *env, oid_t database_oid,
                                     catalog::IndexCatalogObject *index) {
  // TODO: Remove hardcoded database name and server end point.
  auto &client = env->GetPelotonClient();
  PelotonService::Client peloton_service = client.getMain<PelotonService>();

  auto request = peloton_service.dropIndexRequest();
  request.getRequest().setDatabaseOid(database_oid);
  request.getRequest().setIndexOid(index->GetIndexOid());

  auto response = request.send().wait(client.getWaitScope());
}

uint64_t IndexSelectionJob::GetLatestQueryTimestamp(
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
