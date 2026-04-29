#pragma once
#include "duckdb.hpp"
#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>

namespace duckdb {

enum class ExpectationAction {
    WARN,       // Keep bad rows, log count
    DROP_ROW,   // Filter out bad rows
    FAIL_UPDATE // Abort pipeline
};

struct Expectation {
    string name;
    string expression; // SQL boolean expression
    ExpectationAction action;
};

struct MaterializedViewDefinition {
    string name;
    string query;                       // The AS query
    string comment;
    vector<Expectation> expectations;
    vector<string> explicit_dependencies; // DEPENDS ON clause (empty = auto-detect)
    bool is_materialized = false;         // Has been executed at least once
};

class MaterializedViewCatalog {
public:
    void CreateOrRefresh(const string &name, const string &query,
                         const string &comment,
                         const vector<Expectation> &expectations,
                         const vector<string> &depends_on);
    void AlterQuery(const string &name, const string &query);
    void AddConstraint(const string &name, const Expectation &expectation);
    void DropConstraint(const string &name, const string &constraint_name);
    void Drop(const string &name);
    bool Exists(const string &name) const;
    const MaterializedViewDefinition &Get(const string &name) const;
    vector<string> GetAllNames() const;
    void MarkMaterialized(const string &name);
    static MaterializedViewCatalog &Get(DatabaseInstance &db);

private:
    mutable mutex catalog_mutex;
    unordered_map<string, MaterializedViewDefinition> definitions;
};

} // namespace duckdb
