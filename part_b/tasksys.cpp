#include "tasksys.h"
#include <iostream>

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
    task_with_dependencies.notify_one();
    //
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {



    if(!this->pool){
        auto streaming = [&](int threadId) {
            while(true){
                // std::cout << "Running thread " << threadId  << std::endl;


                std::unique_lock<std::mutex> lck(queue_mutex);
                while(runnable_queue.empty() && this->pool_active){
                    // std::cout << "Sleeping thread " << threadId  << std::endl;

                    task_available.wait(lck);
                }

                if(!this->pool_active){
                    return;
                }


                QueuedTask* front_task = runnable_queue.front();
                int my_task  = front_task->num_total_tasks - front_task->tasks_queued;
                // std::cout << "Executing " << my_task  << std::endl;

                front_task->tasks_queued--;

                lck.unlock();
                front_task->runnable->runTask(my_task, front_task->num_total_tasks);
                front_task->m.lock();
                front_task->completed_tasks++;

                if(front_task->completed_tasks == front_task->num_total_tasks){
                    lck.lock();
                    runnable_queue.pop();

                    front_task->finished=true;
                    task_graph_changed.notify_all();
                }
                front_task->m.unlock();


            }

        };

        auto keeper = [&](){
            while(true){
                std::unique_lock<std::mutex> waiting_lock(waiting_mutex);

                // while(waiting_queue.empty() && this->pool_active){
                //     task_with_dependencies.wait_for(waiting_lock, std::chrono::seconds(2));
                // }

                if(!this->pool_active){
                    // std::cout << "Keeper out!" << std::endl;
                    return;
                }

                for (auto const& task : waiting_queue)
                {
                    bool ready_to_be_queued = true;
                    for(TaskID const& dep: task->deps){
                        if (dependency_map.count(dep)){
                            if(!dependency_map[dep]->finished){
                                ready_to_be_queued = false;
                                // std::cout << "Task id " << queued_id  << " needs " << dep << " to be finished" << std::endl;

                                break;
                            }
                        }
                    }
                    if(ready_to_be_queued){
                        // std::cout << "Task id "   << " ready to be queued!" << std::endl;
                        task->queued = true;
                        queue_mutex.lock();
                        runnable_queue.push(task);
                        task_available.notify_all(); // ???
                        queue_mutex.unlock();
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

    std::unique_lock<std::mutex> waiting_lock(waiting_mutex);
    TaskID registered_id = dependency_map.size();
    QueuedTask* new_record = new QueuedTask;
    new_record->runnable = runnable;
    new_record->num_total_tasks = num_total_tasks;
    new_record->tasks_queued = num_total_tasks;
    new_record->deps = deps;
    dependency_map[registered_id] = new_record;
    waiting_queue.push_back(new_record);
    task_with_dependencies.notify_all();
    return registered_id;

}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> lock(queue_mutex);
    while(!(waiting_queue.empty() && runnable_queue.empty())){
        // std::cout << "Cannot sync. Waiting size: " << waiting_queue.size() << " Runnable queue: " << runnable_queue.size() <<  std::endl;
        task_graph_changed.wait_for(lock, std::chrono::seconds(2) );
    }
    // std::cout<< "Done!!!!" << std::endl;

}
