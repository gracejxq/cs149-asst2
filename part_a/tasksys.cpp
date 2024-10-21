#include "tasksys.h"

const int MAX_EXECUTION_CONTEXTS = 8;

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    threads_available = std::min(num_threads, MAX_EXECUTION_CONTEXTS);
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::runThread(IRunnable* runnable, int num_total_tasks, int index, std::vector<int>& lastTask, 
                std::vector<std::atomic<int>>& curTask, std::unordered_set<int>& runningThreads, std::mutex& runningThreadsMutex) {
    bool running = true;
    while (running) {
        // get task from own queue
        int nextTask = curTask[index].fetch_add(1);
        
        if (nextTask <= lastTask[index]) {
            runnable->runTask(nextTask, num_total_tasks);
        } else {  // if no more tasks left, try stealing 
            {
                std::lock_guard<std::mutex> runningThreadsLock(runningThreadsMutex);
                runningThreads.erase(index);
                if (runningThreads.empty()) { // nothing to steal from
                    running = false;
                    break;
                }
            }

            bool found = false;
            int victimThreadIndex;
            while (!found) {
                // pick a victim thread
                { // scope setting for lock guard
                    std::lock_guard<std::mutex> runningThreadsLock(runningThreadsMutex);
                    std::vector<int> runningThreadsVec = std::vector<int>(runningThreads.begin(), runningThreads.end());

                    std::mt19937 randGen(std::random_device{}()); // rand() is not thread safe
                    std::uniform_int_distribution<> distr(0, runningThreadsVec.size() - 1);
                    victimThreadIndex = runningThreadsVec[distr(randGen)];
                }

                // try stealing from victim thread
                int victimThreadNextTask = curTask[victimThreadIndex].fetch_add(1);
                if (victimThreadNextTask <= lastTask[victimThreadIndex]) {
                    runnable->runTask(victimThreadNextTask, num_total_tasks);
                    found = true;
                } else {  // no more tasks in victim thread's queue ==> update state & pick another victim
                    std::lock_guard<std::mutex> runningThreadsLock(runningThreadsMutex);
                    runningThreads.erase(victimThreadIndex);

                    if (runningThreads.empty()) { // nothing to steal from
                        running = false;
                        break;
                    }
                }
            }
        }       
    }
}

void TaskSystemParallelSpawn::runThreadSingleTask(IRunnable* runnable, int task_id, int num_total_tasks) {
    runnable->runTask(task_id, num_total_tasks);
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // run everything with static assignment if possible:
    if (num_total_tasks <= threads_available) {
        std::vector<std::thread> threads(num_total_tasks);
        for (int i = 0; i < num_total_tasks; i++) {
            threads[i] = std::thread(&TaskSystemParallelSpawn::runThreadSingleTask, this, runnable, i, num_total_tasks);
        }

        for (std::thread &t : threads) {
            t.join();
        }
        return;
    }

    // otherwise, run everything with dynamic assignment:
    std::vector<int> lastTask(threads_available); // read only
    std::vector<std::atomic<int>> curTask(threads_available);
    std::unordered_set<int> runningThreads;
    std::mutex runningThreadsMutex;

    int tasksPerThread = num_total_tasks / threads_available;
    int extraTasks = num_total_tasks % threads_available;

    // set up initial assignment
    int curTaskID = 0;
    for (int i = 0; i < threads_available; i++) { 
        int numTasks = tasksPerThread + (i < extraTasks ? 1 : 0);
        curTask[i] = curTaskID;
        lastTask[i] = curTaskID + numTasks - 1;
        curTaskID = lastTask[i] + 1;

        runningThreads.insert(i);
    }

    // launch threads
    std::vector<std::thread> threads(threads_available);
    for (int i = 0; i < threads_available; i++) {
        threads[i] = std::thread(&TaskSystemParallelSpawn::runThread, this, runnable, num_total_tasks, i, 
                                std::ref(lastTask), std::ref(curTask), std::ref(runningThreads), std::ref(runningThreadsMutex));
    }

    // join threads
    for (std::thread &t : threads) {
        t.join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
