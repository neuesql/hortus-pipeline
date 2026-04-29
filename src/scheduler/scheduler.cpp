#include "scheduler/scheduler.hpp"
#include "catalog/materialized_view_catalog.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {

static unique_ptr<PipelineScheduler> global_scheduler_instance;
static std::mutex global_scheduler_mutex;

PipelineScheduler &PipelineScheduler::Get(DatabaseInstance &db) {
    lock_guard<std::mutex> lock(global_scheduler_mutex);
    if (!global_scheduler_instance) {
        global_scheduler_instance = make_uniq<PipelineScheduler>(db);
    }
    return *global_scheduler_instance;
}

PipelineScheduler::PipelineScheduler(DatabaseInstance &db) : db(db) {
    scheduler_thread = std::thread([this]() { RunScheduler(); });
}

PipelineScheduler::~PipelineScheduler() {
    {
        lock_guard<std::mutex> lock(mutex);
        should_stop = true;
    }
    cv.notify_all();
    if (scheduler_thread.joinable()) {
        scheduler_thread.join();
    }
}

void PipelineScheduler::AddSchedule(const string &view_name) {
    lock_guard<std::mutex> lock(mutex);
    if (active_views.count(view_name)) {
        return; // Already scheduled
    }
    auto next = ComputeNextRun(view_name);
    if (next == std::chrono::system_clock::time_point{}) {
        return; // No valid schedule (ON_UPDATE or NONE)
    }
    task_queue.push({view_name, next});
    active_views.insert(view_name);
    cv.notify_one();
}

void PipelineScheduler::RemoveSchedule(const string &view_name) {
    lock_guard<std::mutex> lock(mutex);
    active_views.erase(view_name);
    // Task will be skipped when it fires since it won't be in active_views
    cv.notify_one();
}

void PipelineScheduler::PauseSchedule(const string &view_name) {
    lock_guard<std::mutex> lock(mutex);
    // Catalog handles the paused flag; scheduler checks it at fire time
}

void PipelineScheduler::ResumeSchedule(const string &view_name) {
    lock_guard<std::mutex> lock(mutex);
    // If view is active but was paused, it will resume at next check
    cv.notify_one();
}

vector<PipelineScheduler::ScheduleInfo> PipelineScheduler::ListSchedules() {
    // Read from catalog, not from scheduler internal state
    auto &catalog = MaterializedViewCatalog::Get(db);
    auto names = catalog.GetAllNames();
    vector<ScheduleInfo> result;

    for (auto &name : names) {
        auto &def = catalog.Get(name);
        if (def.schedule_type == 0) {
            continue; // No schedule
        }
        ScheduleInfo info;
        info.name = name;
        info.paused = def.schedule_paused;

        switch (def.schedule_type) {
        case 1: // EVERY
            info.schedule_description = "EVERY " + std::to_string(def.schedule_interval) + " " + def.schedule_interval_unit;
            break;
        case 2: // CRON
            info.schedule_description = "CRON " + def.schedule_cron_expression;
            break;
        case 3: // ON_UPDATE
            info.schedule_description = "TRIGGER ON UPDATE";
            break;
        default:
            break;
        }
        result.push_back(std::move(info));
    }
    return result;
}

std::chrono::system_clock::time_point PipelineScheduler::ComputeNextRun(const string &view_name) {
    auto &catalog = MaterializedViewCatalog::Get(db);
    if (!catalog.Exists(view_name)) {
        return {};
    }
    auto &def = catalog.Get(view_name);

    if (def.schedule_type == 1) { // EVERY
        auto now = std::chrono::system_clock::now();
        int seconds = def.schedule_interval;
        string unit = def.schedule_interval_unit;
        if (unit == "MINUTE") seconds *= 60;
        else if (unit == "HOUR") seconds *= 3600;
        else if (unit == "DAY") seconds *= 86400;
        else if (unit == "WEEK") seconds *= 604800;
        // SECOND is already correct
        return now + std::chrono::seconds(seconds);
    } else if (def.schedule_type == 2) { // CRON
        // Simplified: treat as 1-hour interval for basic cron support
        return std::chrono::system_clock::now() + std::chrono::hours(1);
    }
    // ON_UPDATE (3) or NONE (0): no time-based scheduling
    return {};
}

void PipelineScheduler::RunScheduler() {
    while (true) {
        std::unique_lock<std::mutex> lock(mutex);

        if (should_stop) {
            return;
        }

        if (task_queue.empty()) {
            cv.wait(lock, [this]() { return should_stop || !task_queue.empty(); });
            if (should_stop) {
                return;
            }
            continue;
        }

        auto top = task_queue.top();
        auto now = std::chrono::system_clock::now();

        if (top.next_run > now) {
            cv.wait_until(lock, top.next_run, [this]() { return should_stop; });
            if (should_stop) {
                return;
            }
            continue;
        }

        // Pop the task
        task_queue.pop();

        // Check if still active
        if (active_views.find(top.view_name) == active_views.end()) {
            continue;
        }

        // Check if paused
        auto &catalog = MaterializedViewCatalog::Get(db);
        if (!catalog.Exists(top.view_name)) {
            active_views.erase(top.view_name);
            continue;
        }
        auto &def = catalog.Get(top.view_name);
        if (def.schedule_paused) {
            // Reschedule for later check
            auto next = ComputeNextRun(top.view_name);
            if (next != std::chrono::system_clock::time_point{}) {
                task_queue.push({top.view_name, next});
            }
            continue;
        }

        // Release lock before executing
        lock.unlock();

        // Execute refresh
        try {
            Connection conn(db);
            conn.Query("REFRESH MATERIALIZED VIEW " + top.view_name);
        } catch (...) {
            // Log error but don't crash the scheduler
        }

        // Reschedule
        lock.lock();
        if (active_views.find(top.view_name) != active_views.end()) {
            auto next = ComputeNextRun(top.view_name);
            if (next != std::chrono::system_clock::time_point{}) {
                task_queue.push({top.view_name, next});
            }
        }
    }
}

} // namespace duckdb
