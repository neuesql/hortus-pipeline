#include "catalog/materialized_view_catalog.hpp"
#include "duckdb/common/exception.hpp"
#include <algorithm>

namespace duckdb {

static MaterializedViewCatalog global_catalog_instance;

MaterializedViewCatalog &MaterializedViewCatalog::Get(DatabaseInstance &db) {
    return global_catalog_instance;
}

void MaterializedViewCatalog::CreateOrRefresh(const string &name, const string &query,
                                              const string &comment,
                                              const vector<Expectation> &expectations,
                                              const vector<string> &depends_on) {
    lock_guard<mutex> lock(catalog_mutex);
    MaterializedViewDefinition def;
    def.name = name;
    def.query = query;
    def.comment = comment;
    def.expectations = expectations;
    def.explicit_dependencies = depends_on;
    def.is_materialized = false;
    definitions[name] = std::move(def);
}

void MaterializedViewCatalog::AlterQuery(const string &name, const string &query) {
    lock_guard<mutex> lock(catalog_mutex);
    auto it = definitions.find(name);
    if (it == definitions.end()) {
        throw InvalidInputException("Materialized view '%s' not found", name);
    }
    it->second.query = query;
    it->second.is_materialized = false;
}

void MaterializedViewCatalog::AddConstraint(const string &name, const Expectation &expectation) {
    lock_guard<mutex> lock(catalog_mutex);
    auto it = definitions.find(name);
    if (it == definitions.end()) {
        throw InvalidInputException("Materialized view '%s' not found", name);
    }
    for (const auto &e : it->second.expectations) {
        if (e.name == expectation.name) {
            throw InvalidInputException("Constraint '%s' already exists on materialized view '%s'",
                                        expectation.name, name);
        }
    }
    it->second.expectations.push_back(expectation);
}

void MaterializedViewCatalog::DropConstraint(const string &name, const string &constraint_name) {
    lock_guard<mutex> lock(catalog_mutex);
    auto it = definitions.find(name);
    if (it == definitions.end()) {
        throw InvalidInputException("Materialized view '%s' not found", name);
    }
    auto &exps = it->second.expectations;
    auto exp_it = std::find_if(exps.begin(), exps.end(),
                               [&](const Expectation &e) { return e.name == constraint_name; });
    if (exp_it == exps.end()) {
        throw InvalidInputException("Constraint '%s' not found on materialized view '%s'",
                                    constraint_name, name);
    }
    exps.erase(exp_it);
}

void MaterializedViewCatalog::Drop(const string &name) {
    lock_guard<mutex> lock(catalog_mutex);
    auto it = definitions.find(name);
    if (it == definitions.end()) {
        throw InvalidInputException("Materialized view '%s' not found", name);
    }
    definitions.erase(it);
}

bool MaterializedViewCatalog::Exists(const string &name) const {
    lock_guard<mutex> lock(catalog_mutex);
    return definitions.find(name) != definitions.end();
}

const MaterializedViewDefinition &MaterializedViewCatalog::Get(const string &name) const {
    lock_guard<mutex> lock(catalog_mutex);
    auto it = definitions.find(name);
    if (it == definitions.end()) {
        throw InvalidInputException("Materialized view '%s' not found", name);
    }
    return it->second;
}

vector<string> MaterializedViewCatalog::GetAllNames() const {
    lock_guard<mutex> lock(catalog_mutex);
    vector<string> names;
    names.reserve(definitions.size());
    for (const auto &pair : definitions) {
        names.push_back(pair.first);
    }
    return names;
}

void MaterializedViewCatalog::MarkMaterialized(const string &name) {
    lock_guard<mutex> lock(catalog_mutex);
    auto it = definitions.find(name);
    if (it == definitions.end()) {
        throw InvalidInputException("Materialized view '%s' not found", name);
    }
    it->second.is_materialized = true;
}

void MaterializedViewCatalog::SetExpectationMetrics(const string &name, const vector<ExpectationMetric> &metrics) {
    lock_guard<mutex> lock(catalog_mutex);
    auto it = definitions.find(name);
    if (it == definitions.end()) {
        throw InvalidInputException("Materialized view '%s' not found", name);
    }
    it->second.last_expectation_metrics = metrics;
}

void MaterializedViewCatalog::PauseSchedule(const string &name) {
    lock_guard<mutex> lock(catalog_mutex);
    auto it = definitions.find(name);
    if (it == definitions.end()) {
        throw InvalidInputException("Materialized view '%s' not found", name);
    }
    it->second.schedule_paused = true;
}

void MaterializedViewCatalog::ResumeSchedule(const string &name) {
    lock_guard<mutex> lock(catalog_mutex);
    auto it = definitions.find(name);
    if (it == definitions.end()) {
        throw InvalidInputException("Materialized view '%s' not found", name);
    }
    it->second.schedule_paused = false;
}

} // namespace duckdb
