//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_selection.h
//
// Identification: src/include/brain/index_selection.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "brain/index_selection_context.h"
#include "brain/index_selection_util.h"
#include "catalog/index_catalog.h"
#include "parser/sql_statement.h"

namespace peloton {
namespace brain {

// TODO: Remove these
using namespace parser;
using namespace catalog;

//===--------------------------------------------------------------------===//
// IndexSelection
//===--------------------------------------------------------------------===//
class IndexSelection {
 public:
  IndexSelection(std::shared_ptr<Workload> query_set);
  std::unique_ptr<IndexConfiguration> GetBestIndexes();

private:
  void Enumerate(IndexConfiguration &indexes,
                 IndexConfiguration &picked_indexes,
                      Workload &workload);
  void GetAdmissibleIndexes(SQLStatement *query,
                            IndexConfiguration &indexes);
  void IndexColsParseWhereHelper(const expression::AbstractExpression *where_expr,
                                 IndexConfiguration &config);
  void IndexColsParseGroupByHelper(std::unique_ptr<GroupByDescription> &where_expr,
                                   IndexConfiguration &config);
  void IndexColsParseOrderByHelper(std::unique_ptr<OrderDescription> &order_by,
                                   IndexConfiguration &config);
  std::shared_ptr<IndexObject> AddIndexColumnsHelper(oid_t database,
                                                     oid_t table, std::vector<oid_t> cols);
  double GetCost(IndexConfiguration &config, Workload &workload);
  IndexConfiguration GenMultiColumnIndexes(IndexConfiguration &config, IndexConfiguration &single_column_indexes);
  // members
  std::shared_ptr<Workload> query_set_;
  IndexSelectionContext context_;
};

}  // namespace brain
}  // namespace peloton
