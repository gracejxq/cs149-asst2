#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"

#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <random>
#include <algorithm>
#include <vector>
#include <queue>
#include <set>

// const int MAX_EXECUTION_CONTEXTS = 8;  // machine unique: myth
const int MAX_EXECUTION_CONTEXTS = 1; // machine unique: aws machine
struct BulkTaskLaunch {
    TaskID id;
    std::vector<TaskID> dependents; // notify all dependents when this finishes
    std::vector<TaskID> dependencies; // helps track number of tasks being waited on
    std::atomic<int> dependenciesLeft;
    IRunnable* runnable;
    int numTotalTasks;
    std::atomic<int> curTask;
    std::atomic<int> doneTasks;
    
    BulkTaskLaunch(int _id, IRunnable* _runnable, int _numTotalTasks, std::vector<TaskID> _dependencies) {
        id = _id;
        dependents = {};
        dependencies = _dependencies;
        dependenciesLeft = dependencies.size();
        runnable = _runnable;
        numTotalTasks = _numTotalTasks;
        curTask = 0;
        doneTasks = 0;
    }
};

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    private:
        int threads_available; // tracks optimal number of threads
        std::vector<std::thread> threads;
        std::atomic<bool> endThreadPool{false};

        std::mutex mutex_;
        std::condition_variable taskAvailable; // cv to start running threads after sleeping
        std::condition_variable tasksDone; // cv to return from run after all threads sleeping

        // USED ONLY BY run()
        IRunnable* currRunnable;
        std::atomic<int> curTask{0};
        std::atomic<int> numTotalTasks{0};
        std::atomic<int> doneTasks{0};
        void runThread(); // helper called by run()

        // USED ONLY BY runAsyncWithDeps()
        std::atomic<TaskID> nextID{1};
        std::unordered_map<TaskID, BulkTaskLaunch*> nonPendingLaunches; // map of all non-finished launches
        std::queue<BulkTaskLaunch*> readyLaunches; // queue of ready-to-run launches
        std::unordered_map<TaskID, BulkTaskLaunch*> pendingLaunches; // maps running/ready launches

        std::set<TaskID> doneLaunches; // set of done launches
        void runAsyncWithDepsThread(int i); // helper called by runAsyncWithDeps()
        void updateDependents(std::vector<TaskID> dependents); // helper called by runAsyncWithDeps()


        void printState();

    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

#endif
