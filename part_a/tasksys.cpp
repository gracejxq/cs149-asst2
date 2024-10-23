#include "tasksys.h"
#include <iostream> // TODO: delete later

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

void TaskSystemParallelSpawn::runThread(IRunnable* runnable, const int num_total_tasks, std::atomic<int>& curTask) {
    int myTask;
    while (true) {
        myTask = curTask.fetch_add(1); // atomic save, add, and return
        if (myTask >= num_total_tasks) {
            break;
        }
        runnable->runTask(myTask, num_total_tasks);
    }
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    std::vector<std::thread> threads(threads_available);
    std::atomic<int> curTask(0);

    // launch threads
    for (int i = 0; i < threads_available; i++) {
        threads[i] = std::thread(&TaskSystemParallelSpawn::runThread, this, runnable, num_total_tasks, std::ref(curTask));
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
    endThreadPool = false;
    threads_available = std::min(num_threads, MAX_EXECUTION_CONTEXTS);
    for (int i = 0; i < threads_available; i++) {
        threads.emplace_back(std::thread(&TaskSystemParallelThreadPoolSpinning::runThread, this));
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    endThreadPool = true;
    for (std::thread &t : threads) {
        t.join();
    }
}

void TaskSystemParallelThreadPoolSpinning::runThread() {
    while (!endThreadPool) {
        std::unique_lock<std::mutex> runningThreadsLock(mutex_);
        if (curTask < numTotalTasks) {
            int myTask = curTask.fetch_add(1);
            runningThreadsLock.unlock();
            currRunnable->runTask(myTask, numTotalTasks);
            doneTasks++;
        }
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    {   // scoping to make this atomic in case of spurious wakeups
        std::unique_lock<std::mutex> lock(mutex_);
        currRunnable = runnable;
        curTask = 0;
        doneTasks = 0;
        numTotalTasks = num_total_tasks;
        lock.unlock();
    }
    // currRunnable = runnable;
    // curTask = 0;
    // doneTasks = 0;
    // numTotalTasks = num_total_tasks;
    while (doneTasks < numTotalTasks) {
        continue;
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
    endThreadPool = false;
    curTask = 0;
    doneTasks = 0;
    numTotalTasks = 0;

    threads_available = std::min(num_threads, MAX_EXECUTION_CONTEXTS);
    for (int i = 0; i < threads_available; i++) {
        threads.emplace_back(std::thread(&TaskSystemParallelThreadPoolSleeping::runThread, this, i));
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    {
        std::unique_lock<std::mutex> lock(mutex_);
        endThreadPool = true;
    }

    taskAvailable.notify_all(); // wake all sleeping threads and break them

    for (std::thread &t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::runThread(int id) {
    while (true) {

        // wait until (1) can run new task or (2) destructor is called (endThreadPool)
        std::unique_lock<std::mutex> lock(mutex_);
        while (!endThreadPool && curTask >= numTotalTasks) { // checks against spurious wakeups
            taskAvailable.wait(lock);  
        }

        if (endThreadPool) { // kill thread if destructor
            break; 
        } 

        int myTask = curTask.fetch_add(1);
        if (myTask < numTotalTasks) { // otherwise run next task
            currRunnable->runTask(myTask, numTotalTasks);

            lock.lock();

            // wake main thread to return from run after last task
            if (++doneTasks == numTotalTasks) {
                tasksDone.notify_one();
            }
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    // make this atomic in case of spurious worker thread wakeups
    // std::unique_lock<std::mutex> lock(mutex_);
    currRunnable = runnable;
    curTask = 0;
    doneTasks = 0;
    numTotalTasks = num_total_tasks;

    taskAvailable.notify_all();  // wake all threads to start running tasks

    std::unique_lock<std::mutex> lock(mutex_);
    while (doneTasks < numTotalTasks) { // checks against spurious wakeups
        tasksDone.wait(lock);
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
