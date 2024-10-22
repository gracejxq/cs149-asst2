#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"

#include <thread>
#include <atomic>
#include <mutex>
#include <random>
#include <algorithm>
#include <vector>
#include <queue>

// const int MAX_EXECUTION_CONTEXTS = 8;  // machine unique: myth
const int MAX_EXECUTION_CONTEXTS = 16; // machine unique: aws machine
const int TASK_BATCH = 10;             // each thread claims 10 tasks to run at once (empirically seems to get good results)

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
    private:
        int threads_available; // tracks optimal number of threads
        void runThread(IRunnable* runnable, int num_total_tasks, int index, std::vector<int>& lastTask, std::vector<std::atomic<int>>& curTask,
                                std::vector<std::atomic<bool>>& potentialVictims, std::mutex& potentialVictimMutex); // helper called by run()
        void runThreadSingleTask(IRunnable* runnable, int task_id, int num_total_tasks); // helper called by run()
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
    private:
        int threads_available; // tracks optimal number of threads
        std::vector<std::thread> threads;
        // std::atomic<int> tasks_remaining; // TODO: delete
        std::atomic<bool> stopPool;
        std::vector<int> lastTask; // read only
        std::vector<std::atomic<int>> curTask;
        std::vector<std::atomic<bool>> potentialVictims;
        std::mutex potentialVictimMutex;
        void runThread(IRunnable* runnable, int num_total_tasks, int index); // helper called by run()
        void runThreadSingleTask(IRunnable* runnable, int task_id, int num_total_tasks); // helper called by run()
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
        bool threads_made; // tracks if threads have alr been spawned by a prev call to run()
        std::queue<int> run_queue; // tracks future bulk task launches
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
