#include "scheduler/scheduler.hpp"
#include "persistence/pipeline_persistence.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {

static unique_ptr<PipelineScheduler>
    global_scheduler_instance;            // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
static std::mutex global_scheduler_mutex; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

PipelineScheduler &PipelineScheduler::Get(DatabaseInstance &db) {
	lock_guard<std::mutex> lock(global_scheduler_mutex);
	if (!global_scheduler_instance || &global_scheduler_instance->db != &db) {
		global_scheduler_instance.reset();
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
		return;
	}
	auto resolved = PipelinePersistence::ResolveQualifiedName(view_name);
	auto &database = resolved.first;
	auto &name = resolved.second;
	auto next = ComputeNextRun(database, name);
	if (next == std::chrono::system_clock::time_point {}) {
		return;
	}
	task_queue.push({view_name, database, name, next});
	active_views.insert(view_name);
	cv.notify_one();
}

void PipelineScheduler::RemoveSchedule(const string &view_name) {
	lock_guard<std::mutex> lock(mutex);
	active_views.erase(view_name);
	cv.notify_one();
}

void PipelineScheduler::PauseSchedule(const string &view_name) {
	lock_guard<std::mutex> lock(mutex);
}

void PipelineScheduler::ResumeSchedule(const string &view_name) {
	lock_guard<std::mutex> lock(mutex);
	cv.notify_one();
}

std::chrono::system_clock::time_point PipelineScheduler::ComputeNextRun(const string &database, const string &name) {
	auto &persistence = PipelinePersistence::Get();
	if (!persistence.Exists(db, database, name)) {
		return {};
	}
	auto def = persistence.GetView(db, database, name);

	if (def.schedule_type == 1) { // EVERY
		auto now = std::chrono::system_clock::now();
		int seconds = def.schedule_interval;
		string unit = def.schedule_interval_unit;
		if (unit == "MINUTE") {
			seconds *= 60;
		} else if (unit == "HOUR") {
			seconds *= 3600;
		} else if (unit == "DAY") {
			seconds *= 86400;
		} else if (unit == "WEEK") {
			seconds *= 604800;
		}
		return now + std::chrono::seconds(seconds);
	} else if (def.schedule_type == 2) { // CRON
		return std::chrono::system_clock::now() + std::chrono::hours(1);
	}
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

		task_queue.pop();

		if (active_views.find(top.view_name) == active_views.end()) {
			continue;
		}

		// Check if paused (use stored database qualifier)
		auto &persistence = PipelinePersistence::Get();
		if (!persistence.Exists(db, top.database, top.name)) {
			active_views.erase(top.view_name);
			continue;
		}
		auto def = persistence.GetView(db, top.database, top.name);
		if (def.schedule_paused) {
			auto next = ComputeNextRun(top.database, top.name);
			if (next != std::chrono::system_clock::time_point {}) {
				task_queue.push({top.view_name, top.database, top.name, next});
			}
			continue;
		}

		lock.unlock();

		// Execute refresh
		try {
			Connection conn(db);
			conn.Query("REFRESH MATERIALIZED VIEW " + top.view_name);
			persistence.UpdateScheduleLastRun(db, top.database, top.name);
		} catch (...) {
		}

		lock.lock();
		if (active_views.find(top.view_name) != active_views.end()) {
			auto next = ComputeNextRun(top.database, top.name);
			if (next != std::chrono::system_clock::time_point {}) {
				task_queue.push({top.view_name, top.database, top.name, next});
			}
		}
	}
}

} // namespace duckdb
