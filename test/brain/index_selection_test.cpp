//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_selection_test.cpp
//
// Identification: test/brain/index_selection_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/what_if_index.h"
#include "brain/index_selection_util.h"
#include "brain/index_selection.h"
#include "catalog/index_catalog.h"
#include "common/harness.h"
#include "binder/bind_node_visitor.h"
#include "concurrency/transaction_manager_factory.h"
#include "optimizer/stats/column_stats.h"
#include "optimizer/stats/stats_storage.h"
#include "optimizer/stats/table_stats.h"
#include "sql/testing_sql_util.h"

namespace peloton {

// TODO [vamshi]: remove these
using namespace brain;
using namespace catalog;

namespace test {

// TODO [vamshi]: remove these
using namespace optimizer;

//===--------------------------------------------------------------------===//
// IndexSelectionTest
//===--------------------------------------------------------------------===//

class IndexSelectionTest : public PelotonTest {
 private:
  std::string database_name;

 public:
  IndexSelectionTest() { database_name = DEFAULT_DB_NAME; }

  // Create a new database
  void CreateDatabase() {
    // Create a new database.
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->CreateDatabase(database_name, txn);
    txn_manager.CommitTransaction(txn);
  }

  // Create a new table with schema (a INT, b INT, c INT).
  void CreateTable(std::string table_name) {
    std::string create_str =
        "CREATE TABLE " + table_name + "(a INT, b INT, c INT);";
    TestingSQLUtil::ExecuteSQLQuery(create_str);
  }
};

TEST_F(IndexSelectionTest, BasicTest) {
  std::string table_name = "dummy_table_whatif";
  std::string database_name = DEFAULT_DB_NAME;

  CreateDatabase();

  CreateTable(table_name);

  std::ostringstream oss;
  oss << "SELECT * FROM " << table_name << " WHERE a < 1 or b > 4 and c = 3";

  auto parser = parser::PostgresParser::GetInstance();
  std::unique_ptr<parser::SQLStatementList> stmt_list(
    parser.BuildParseTree(oss.str()).release());
  EXPECT_TRUE(stmt_list->is_valid);

  auto select_stmt = (parser::SelectStatement *)stmt_list->GetStatement(0);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<binder::BindNodeVisitor> binder(
    new binder::BindNodeVisitor(txn, database_name));

  binder->BindNameToNode(select_stmt);

  LOG_INFO("%s", stmt_list->GetInfo().c_str());

  Workload w;
  w.AddQuery(select_stmt);

  IndexSelection is(w);
  IndexConfiguration ic;
  is.GetAdmissibleIndexes(select_stmt, ic);

  LOG_INFO("Got indexes count: %zu", ic.GetIndexCount());
  auto indexes = ic.GetIndexes();

  for (auto it = indexes.begin(); it != indexes.end(); it++) {
    LOG_INFO("%s\n", it->get()->toString().c_str());
  }

  EXPECT_EQ(ic.GetIndexCount(), 3);

  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
