#include "executor/dag_resolver.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include <queue>
#include <unordered_set>
#include <unordered_map>
#include <regex>

namespace duckdb {

vector<string> DAGResolver::ExtractDependencies(const string &query) {
    // Skip known function names
    static const unordered_set<string> skip_names = {
        "read_csv", "read_parquet", "read_json", "range",
        "generate_series", "values", "unnest", "read_csv_auto"
    };

    vector<string> deps;
    unordered_set<string> seen;

    // Find FROM table_name and JOIN table_name patterns
    // We use a simple regex approach
    string upper_query = StringUtil::Upper(query);

    // Pattern: FROM or JOIN followed by a table name (identifier)
    std::regex pattern(R"((?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*))", std::regex::icase);

    auto begin = std::sregex_iterator(query.begin(), query.end(), pattern);
    auto end = std::sregex_iterator();

    for (auto it = begin; it != end; ++it) {
        string table_name = (*it)[1].str();
        string lower_name = StringUtil::Lower(table_name);

        // Skip known functions and already-seen names
        if (skip_names.count(lower_name) > 0) {
            continue;
        }
        if (seen.count(lower_name) > 0) {
            continue;
        }
        seen.insert(lower_name);
        deps.push_back(table_name);
    }

    return deps;
}

vector<string> DAGResolver::Resolve(const MaterializedViewCatalog &catalog) {
    auto all_names = catalog.GetAllNames();
    unordered_set<string> mv_set(all_names.begin(), all_names.end());

    // Build adjacency list: edges[A] = {B, C} means A depends on B, C
    unordered_map<string, vector<string>> dependents; // dependency -> list of MVs that depend on it
    unordered_map<string, int> in_degree;

    for (auto &name : all_names) {
        in_degree[name] = 0;
    }

    for (auto &name : all_names) {
        auto &def = catalog.Get(name);
        vector<string> deps;
        if (!def.explicit_dependencies.empty()) {
            deps = def.explicit_dependencies;
        } else {
            deps = ExtractDependencies(def.query);
        }

        for (auto &dep : deps) {
            if (mv_set.count(dep) > 0 && dep != name) {
                dependents[dep].push_back(name);
                in_degree[name]++;
            }
        }
    }

    // Kahn's algorithm
    std::queue<string> q;
    for (auto &name : all_names) {
        if (in_degree[name] == 0) {
            q.push(name);
        }
    }

    vector<string> result;
    while (!q.empty()) {
        auto current = q.front();
        q.pop();
        result.push_back(current);

        for (auto &dep : dependents[current]) {
            in_degree[dep]--;
            if (in_degree[dep] == 0) {
                q.push(dep);
            }
        }
    }

    if (result.size() != all_names.size()) {
        throw InvalidInputException("Cycle detected in materialized view dependencies");
    }

    return result;
}

vector<string> DAGResolver::ResolveFor(const MaterializedViewCatalog &catalog, const string &target) {
    auto all_names = catalog.GetAllNames();
    unordered_set<string> mv_set(all_names.begin(), all_names.end());

    if (mv_set.count(target) == 0) {
        throw InvalidInputException("Materialized view '%s' not found", target);
    }

    // Build dependency map: name -> set of MV dependencies
    unordered_map<string, vector<string>> dep_map;
    for (auto &name : all_names) {
        auto &def = catalog.Get(name);
        vector<string> deps;
        if (!def.explicit_dependencies.empty()) {
            deps = def.explicit_dependencies;
        } else {
            deps = ExtractDependencies(def.query);
        }
        vector<string> mv_deps;
        for (auto &d : deps) {
            if (mv_set.count(d) > 0 && d != name) {
                mv_deps.push_back(d);
            }
        }
        dep_map[name] = mv_deps;
    }

    // BFS to find all transitive upstream deps of target
    unordered_set<string> needed;
    std::queue<string> bfs;
    bfs.push(target);
    needed.insert(target);
    while (!bfs.empty()) {
        auto current = bfs.front();
        bfs.pop();
        for (auto &dep : dep_map[current]) {
            if (needed.count(dep) == 0) {
                needed.insert(dep);
                bfs.push(dep);
            }
        }
    }

    // Get full topo sort and filter to needed
    auto full_order = Resolve(catalog);
    vector<string> result;
    for (auto &name : full_order) {
        if (needed.count(name) > 0) {
            result.push_back(name);
        }
    }

    return result;
}

} // namespace duckdb
