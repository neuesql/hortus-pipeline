#include "executor/dag_resolver.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include <queue>
#include <unordered_set>
#include <unordered_map>
#include <regex>

namespace duckdb {

vector<string> DAGResolver::ExtractDependencies(const string &query) {
	static const unordered_set<string> skip_names = {"read_csv",        "read_parquet", "read_json", "range",
	                                                 "generate_series", "values",       "unnest",    "read_csv_auto"};

	vector<string> deps;
	unordered_set<string> seen;

	std::regex pattern(R"((?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*))", std::regex::icase);

	auto begin = std::sregex_iterator(query.begin(), query.end(), pattern);
	auto end = std::sregex_iterator();

	for (auto it = begin; it != end; ++it) {
		string table_name = (*it)[1].str();
		string lower_name = StringUtil::Lower(table_name);

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

vector<string> DAGResolver::ResolveEffectiveDeps(const MaterializedViewDefinition &def,
                                                 const unordered_set<string> &mv_set) {
	vector<string> raw;
	if (!def.explicit_dependencies.empty()) {
		raw = def.explicit_dependencies;
	} else {
		raw = ExtractDependencies(def.query);
	}
	vector<string> result;
	for (auto &dep : raw) {
		if (mv_set.count(dep) > 0 && dep != def.name) {
			result.push_back(dep);
		}
	}
	return result;
}

vector<string> DAGResolver::Resolve(DatabaseInstance &db, const string &database) {
	auto &persistence = PipelinePersistence::Get();
	auto all_names = persistence.GetAllNames(db, database);
	unordered_set<string> mv_set(all_names.begin(), all_names.end());

	unordered_map<string, vector<string>> dependents;
	unordered_map<string, int> in_degree;

	for (auto &name : all_names) {
		in_degree[name] = 0;
	}

	for (auto &name : all_names) {
		auto def = persistence.GetView(db, database, name);
		auto deps = ResolveEffectiveDeps(def, mv_set);
		for (auto &dep : deps) {
			dependents[dep].push_back(name);
			in_degree[name]++;
		}
	}

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

vector<string> DAGResolver::ResolveFor(DatabaseInstance &db, const string &target, const string &database) {
	auto &persistence = PipelinePersistence::Get();
	auto all_names = persistence.GetAllNames(db, database);
	unordered_set<string> mv_set(all_names.begin(), all_names.end());

	if (mv_set.count(target) == 0) {
		throw InvalidInputException("Materialized view '%s' not found", target);
	}

	unordered_map<string, vector<string>> dep_map;
	for (auto &name : all_names) {
		auto def = persistence.GetView(db, database, name);
		dep_map[name] = ResolveEffectiveDeps(def, mv_set);
	}

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

	auto full_order = Resolve(db, database);
	vector<string> result;
	for (auto &name : full_order) {
		if (needed.count(name) > 0) {
			result.push_back(name);
		}
	}

	return result;
}

} // namespace duckdb
