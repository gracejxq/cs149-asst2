#include "tasksys.h"


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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
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
        threads.emplace_back(std::thread(&TaskSystemParallelThreadPoolSleeping::runAsyncWithDepsThread, this, i));
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

    for (auto &pair : nonPendingLaunches) {
        delete pair.second; // destroy all remaining launch structs (heap-allocated) for async
    }
}

void TaskSystemParallelThreadPoolSleeping::runThread() {
    while (true) {

        // wait until (1) can run new task or (2) destructor is called (endThreadPool)
        std::unique_lock<std::mutex> lock(mutex_);
        while (!endThreadPool && curTask >= numTotalTasks) { // checks against spurious wakeups
            taskAvailable.wait(lock);  
        }

        bool killThread = endThreadPool;
        lock.unlock();

        if (killThread) { // kill thread if destructor called
            break; 
        } 

        int myTask = curTask.fetch_add(1);
        if (myTask < numTotalTasks) { // otherwise run next task if possible
            currRunnable->runTask(myTask, numTotalTasks);
            doneTasks.fetch_add(1);

            lock.lock();
            if (doneTasks == numTotalTasks) { // wake main thread after last task
                tasksDone.notify_all();
            }
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    {  // make this atomic in case of spurious worker thread wakeups
        std::unique_lock<std::mutex> lock(mutex_);
        currRunnable = runnable;
        curTask = 0;
        doneTasks = 0;
        numTotalTasks = num_total_tasks;
    }

    taskAvailable.notify_all();  // wake all threads to start running tasks

    std::unique_lock<std::mutex> lock(mutex_);
    while (doneTasks < numTotalTasks) { // checks against spurious wakeups
        tasksDone.wait(lock);
    }
}

void TaskSystemParallelThreadPoolSleeping::updateDependents(std::vector<TaskID> dependents) {
    for (TaskID dep : dependents) {
        std::unique_lock<std::mutex> lock(mutex_);
        BulkTaskLaunch* depLaunch = nonPendingLaunches[dep];
        if (!depLaunch) {
            continue;
        }
        depLaunch->dependenciesLeft--;
        if (depLaunch->dependenciesLeft == 0) {
            nonPendingLaunches.erase(dep);
            readyLaunches.push(depLaunch);
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::runAsyncWithDepsThread(int i) {
    while (true) {
        BulkTaskLaunch* curLaunch = nullptr;
        
        // wait until (1) can run new bulk task launch or (2) destructor is called (endThreadPool)
        std::unique_lock<std::mutex> lock(mutex_);
        while (!endThreadPool && readyLaunches.empty()) { // checks against spurious wakeups
            printf("Thread %d waiting\n", i);
            taskAvailable.wait(lock);  
        }

        if (endThreadPool) { // kill thread if destructor called
            printf("Thread %d breaking\n", i);
            break; 
        } 

        curLaunch = readyLaunches.front();
        pendingLaunches[curLaunch->id] = curLaunch;

        int myTask = curLaunch->curTask.fetch_add(1);
        if (myTask < curLaunch->numTotalTasks) { // otherwise run next task if possible
            if (myTask + 1 == curLaunch->numTotalTasks) { // last task, so remove this bulk launch
                printf("Thread %d removing bulk launch %d from ready queue\n", i, curLaunch->id);
                readyLaunches.pop();
            }
            lock.unlock();

            printf("Thread %d running task %d from bulk launch %d with %d total tasks\n", i, myTask, curLaunch->id, curLaunch->numTotalTasks);
            curLaunch->runnable->runTask(myTask, curLaunch->numTotalTasks);

            lock.lock();
            curLaunch->doneTasks.fetch_add(1);
            if (curLaunch->doneTasks == pendingLaunches[curLaunch->id]->numTotalTasks) { // if all tasks are finished, update state
                printf("Thread %d done with bulk launch %d\n", i, curLaunch->id);
                doneLaunches.insert(curLaunch->id);
                pendingLaunches.erase(curLaunch->id);
                // update dependents
                updateDependents(curLaunch->dependents);

                delete curLaunch;
                tasksDone.notify_all();
            }
        } /*else {
            std::unique_lock<std::mutex> lock(mutex_);
            if (!readyLaunches.empty() && readyLaunches.front() == curLaunch) {
                readyLaunches.pop();
            }
        }*/
    }
}

void TaskSystemParallelThreadPoolSleeping::printState() { 
    std::unique_lock<std::mutex> lock(mutex_);
    // printf("   Pending bulk launches: ");
    // for (auto const pending : pendingLaunches) {
    //     printf("%d ", pending.second->id);
    // }
    // printf("\n");

    // printf("   NonPending bulk launches: ");
    // for (auto const pending : nonPendingLaunches) {
    //     printf("%d ", pending.second->id);
    // }
    // printf("\n");

    printf("   Ready bulk launches: %zu\n", readyLaunches.size());
    printf("   Pending bulk launches: %zu\n", pendingLaunches.size());
    printf("   NonPending bulk launches: %zu\n", nonPendingLaunches.size());
    printf("   Done bulk launches: %zu\n", doneLaunches.size());
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks, const std::vector<TaskID>& deps) {
    TaskID myID = nextID.fetch_add(1);
    BulkTaskLaunch* myLaunch = new BulkTaskLaunch(myID, runnable, num_total_tasks, deps);

    // std::unique_lock<std::mutex> lock(mutex_);
    printf("New bulk launch %d received\n", myID);
    printState();
    // lock.unlock();

    {   // atomically update thread pool state
        std::unique_lock<std::mutex> lock(mutex_);

        int dependenciesLeft = 0;
        for (TaskID depID : deps) {
            if (nonPendingLaunches.find(depID) != nonPendingLaunches.end()) {
                dependenciesLeft++;
            }
            auto it = nonPendingLaunches.find(depID);
            if (it != nonPendingLaunches.end()) {
                it->second->dependents.push_back(myID);
            }
        }
        
        if (dependenciesLeft == 0) { // if not waiting on dependencies, run
            printf("Pushing new bulk launch %d into ready queue\n", myID);
            readyLaunches.push(myLaunch);
            pendingLaunches[myID] = myLaunch;
            taskAvailable.notify_all();
        } else {
            nonPendingLaunches[myID] = myLaunch;
        }
    }

    return myID;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    TaskID sync_id;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        sync_id = nextID.load() - 1;
    }

    std::unique_lock<std::mutex> lock(mutex_);
    while (true) {
        bool allCompleted = true;
        for (const auto& pair : nonPendingLaunches) {
            TaskID id = pair.first;
            if (id <= sync_id) {
                allCompleted = false;
                break;
            }
        }
        if (allCompleted) {
            break; // All required launches are completed
        }

        while (!pendingLaunches.empty()) {
            tasksDone.wait(lock); // sleep until the next bulk launch finishes
        }
    }
    return;
}
