//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// configuration.h
//
// Identification: src/include/brain/configuration.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <set>
#include <sstream>
#include <string.h>
#include "catalog/index_catalog.h"
#include "parser/sql_statement.h"


namespace peloton {
namespace brain {

using namespace parser;

// Represents a hypothetical index
class IndexObject {
public:
  oid_t db_oid;
  oid_t table_oid;
  std::vector<oid_t> column_oids;
  IndexConstraintType type;

  IndexObject() {};

  IndexObject(oid_t db_oid, oid_t table_oid, oid_t col_oid):
    db_oid(db_oid), table_oid(table_oid) {
    column_oids.push_back(col_oid);
  }

  // To string for performing hash.
  const std::string toString() const {
    std::stringstream str_stream;
    str_stream << db_oid << table_oid;
    for (auto col: column_oids) {
      str_stream << col;
    }
    return str_stream.str();
  }

  bool operator==(const IndexObject &obj) const {
    if (db_oid == obj.db_oid && table_oid == obj.table_oid
        && column_oids == obj.column_oids) {
      return true;
    }
    return false;
  }
};

struct IndexObjectHasher {
  size_t operator()(const IndexObject &obj) const {
    return std::hash<std::string>()(obj.toString());
  }
};

// Represents a set of hypothetical indexes - An index configuration.
class IndexConfiguration {
public:
  IndexConfiguration();
  void Add(IndexConfiguration &config);
  void AddIndexObject(std::shared_ptr<IndexObject> index_info);
  size_t GetIndexCount();
  std::set<std::shared_ptr<IndexObject>> &GetIndexes();
private:
  // The set of hypothetical indexes in the configuration
  std::set<std::shared_ptr<IndexObject>> indexes_;
};

// Represents a workload of SQL queries
class Workload {
private:
  std::vector<SQLStatement*> sql_queries_;
public:
  Workload() {}
  void AddQuery(SQLStatement *query) {
    sql_queries_.push_back(query);
  }
  std::vector<SQLStatement*> &GetQueries() {
    return sql_queries_;
  }
  size_t Size() {
    return sql_queries_.size();
  }
};

class IndexObjectPool {
public:
  IndexObjectPool();
  std::shared_ptr<IndexObject> GetIndexObject(IndexObject &obj);
  std::shared_ptr<IndexObject> PutIndexObject(IndexObject &obj);
private:
  std::unordered_map<IndexObject, std::shared_ptr<IndexObject>, IndexObjectHasher> map_;
};

}  // namespace brain
}  // namespace peloton
