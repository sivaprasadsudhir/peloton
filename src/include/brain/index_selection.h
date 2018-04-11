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

#include "index_selection_util.h"
#include "parser/sql_statement.h"
#include "catalog/index_catalog.h"
#include "brain/index_selection_util.h"
#include "brain/index_selection_context.h"
#include "expression/tuple_value_expression.h"

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
  void IndexObjectPoolInsertHelper(const expression::TupleValueExpression *tuple_col);
  // members
  std::shared_ptr<Workload> query_set_;
  IndexSelectionContext context_;
};

}  // namespace brain
}  // namespace peloton
