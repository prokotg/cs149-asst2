#include "tasksys.h"
#include <iostream>
#include "CycleTimer.h"
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
    this->pool_active=true;
    this->num_threads = num_threads;

}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).

    this->pool_active = false;
    task_available.notify_all();
    for (int i = 0; i < this->num_threads; i++) {
            // std::cout<< "Waiting for " << i << std::endl;
            pool[i].join();
    }
    // std::cout << "Finished!" << std::endl;
    task_graph_changed.notify_one();
    //
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

void TaskSystemParallelThreadPoolSleeping::worker(int threadId){
    return;
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {



    if(!this->pool){
        auto streaming = [&](int threadId) {
            while(true){
                std::unique_lock<std::mutex> lck(queue_mutex);

                while(runnable_queue.empty() && this->pool_active){
                    task_available.wait(lck);
                }
                // std::cout << "Thread " << threadId << "active" << std::endl;

                if(!this->pool_active){
                    return;
                }
                RunnableTask * front_task = runnable_queue.front();
                runnable_queue.pop();
                lck.unlock();
                // std::cout << "Thread " << threadId << "Running runnable " << front_task->registered_id << " task " << front_task->task_id << std::endl;
                front_task->runnable->runTask(front_task->task_id, front_task->num_total_tasks);
                auto dd = dependency_map[front_task->registered_id];
                std::unique_lock<std::mutex> lock(dependency_map[front_task->registered_id]->m);
                dependency_map[front_task->registered_id]->completed_tasks++;
                if(dependency_map[front_task->registered_id]->completed_tasks == dependency_map[front_task->registered_id]->num_total_tasks){
                    // std::cout << "Thread " << threadId << " mark runnable  " << front_task->registered_id << " complete with task " << front_task->task_id << std::endl;

                    dependency_map[front_task->registered_id]->finished=true;
                    endm.lock();
                    completed_runnables++;
                    endm.unlock();
                    task_graph_changed.notify_all(); //??

                }
                lock.unlock();

                }


        };

        auto keeper = [&](){
            while(true){
                std::unique_lock<std::mutex> waiting_lock(waiting_mutex);

                while(waiting_queue.empty() && this->pool_active){
                    task_graph_changed.wait(waiting_lock);
                }

                if(!this->pool_active){
                    // std::cout << "Keeper out!" << std::endl;
                    return;
                }

                for (auto const& task : waiting_queue)
                {
                    bool ready_to_be_queued = true;
                    for(TaskID const& dep: task->deps){
                            if(!dependency_map[dep]->finished){
                                // std::cout << "Task id "  << task->registered_id << " need dependency " << dep << std::endl;

                                ready_to_be_queued = false;
                                break;
                            }
                    }
                    if(ready_to_be_queued){
                        // std::cout << "Runnable id "  << task->registered_id << " ready to be queued!" << std::endl;
                        task->queued = true;
                        queue_mutex.lock();
                        for(int i = 0; i < task->num_total_tasks; ++i ){
                            RunnableTask* rt = new RunnableTask();
                            rt->runnable = task->runnable;
                            rt->num_total_tasks = task->num_total_tasks;
                            rt->task_id=i;
                            rt->registered_id=task->registered_id;
                            runnable_queue.push(rt);

                        }
                        queue_mutex.unlock();
                        task_available.notify_all(); // ???

                    }
                }
                waiting_queue.remove_if([](QueuedTask* task){return task->queued;});

            }
        };
        pool = new std::thread[num_threads];
        bookkeeper = new std::thread(keeper);

        for (int i = 0; i < this->num_threads; i++) {
            pool[i] =  std::thread(streaming, i);
        }
    };

    TaskID registered_id = dependency_map.size();
    QueuedTask* new_record = new QueuedTask;
    new_record->runnable = runnable;
    new_record->num_total_tasks = num_total_tasks;
    new_record->deps = deps;
    new_record->registered_id=registered_id;
    dependency_map[registered_id] = new_record;
    std::unique_lock<std::mutex> waiting_lock(waiting_mutex);
    waiting_queue.push_back(new_record);
    waiting_lock.unlock();

    task_graph_changed.notify_all();
    endm.lock();
    queued_runnables++;
    endm.unlock();
    return registered_id;

}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> lock(endm);
    while(completed_runnables!=queued_runnables){
        // std::cout << "Cannot sync. Waiting size: " << waiting_queue.size() << " Runnable queue: " << runnable_queue.size() <<  std::endl;
        // std::cout << "Completed " << completed_runnables << " Queued " << queued_runnables << std::endl;
        task_graph_changed.wait(lock);
    }
    // std::cout<< "Done!!!!" << std::endl;

}
