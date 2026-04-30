#pragma once
#include "duckdb.hpp"
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <unordered_set>
#include <chrono>

namespace duckdb {

class PipelineScheduler {
public:
    explicit PipelineScheduler(DatabaseInstance &db);
    ~PipelineScheduler();

    // view_name can be qualified: "db.name" or just "name"
    void AddSchedule(const string &view_name);
    void RemoveSchedule(const string &view_name);
    void PauseSchedule(const string &view_name);
    void ResumeSchedule(const string &view_name);

    static PipelineScheduler &Get(DatabaseInstance &db);

private:
    void RunScheduler();
    std::chrono::system_clock::time_point ComputeNextRun(const string &database, const string &name);

    DatabaseInstance &db;
    std::thread scheduler_thread;
    std::mutex mutex;
    std::condition_variable cv;
    bool should_stop = false;

    struct ScheduledTask {
        string view_name;  // qualified name for REFRESH command
        string database;   // database qualifier for persistence lookups
        string name;       // unqualified name for persistence lookups
        std::chrono::system_clock::time_point next_run;
        bool operator>(const ScheduledTask &other) const { return next_run > other.next_run; }
    };
    std::priority_queue<ScheduledTask, vector<ScheduledTask>, std::greater<ScheduledTask>> task_queue;
    unordered_set<string> active_views;
};

} // namespace duckdb
