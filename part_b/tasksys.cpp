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
    queued_runnables = 0;
    completed_runnables = 0;

    pool = new std::thread[num_threads-1];


    for (int i = 0; i < this->num_threads-1; i++) {
        pool[i] =  std::thread(&TaskSystemParallelThreadPoolSleeping::worker, this, i);
    }

    bookkeeper = new std::thread(&TaskSystemParallelThreadPoolSleeping::keeper, this);

}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).

    this->pool_active = false;
    task_available.notify_all();
    for (int i = 0; i < this->num_threads-1; i++) {
            // std::cout<< "Waiting for " << i << std::endl;
            pool[i].join();
    }
    // std::cout << "Finished!" << std::endl;
    task_with_dependencies.notify_one();
    //
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    const std::vector<TaskID> noDeps{};
    runAsyncWithDeps(runnable, num_total_tasks, noDeps);
    sync();



}

void TaskSystemParallelThreadPoolSleeping::worker(int threadId){

    while(true){

        std::unique_lock<std::mutex> lck(queue_mutex);
        while(runnable_queue.empty() && this->pool_active){

            task_available.wait(lck);
        }
        if(!this->pool_active){
            return;
        }

        RunnableChunk * front_task = runnable_queue.front();
        runnable_queue.pop();
        lck.unlock();
        front_task->runnable->runTask(front_task->task_id, front_task->num_total_tasks);
        front_task->bulk->completed_tasks++;


    }

}


void TaskSystemParallelThreadPoolSleeping::keeper(){
    while(true){
                std::unique_lock<std::mutex> waiting_lock(waiting_mutex);

                if(!this->pool_active){
                    return;
                }

                // go over running tasks and see if any of it is finished
                for (auto const& task : running_queue){
                    if(task->completed_tasks == task->num_total_tasks){
                        task->finished = true;
                        completed_runnables++;
                        task_graph_changed.notify_all();
                    }
                }

                running_queue.remove_if([](QueuedTask* task){return task->finished;});


                for (auto const& task : waiting_queue)
                {
                    bool ready_to_be_queued = true;
                    for(TaskID const& dep: task->deps){
                        if (dependency_map.count(dep)){
                            if(!dependency_map[dep]->finished){
                                ready_to_be_queued = false;

                                break;
                            }
                        }
                    }
                    if(ready_to_be_queued){
                        task->queued = true;
                        queue_mutex.lock();
                        for(int i =0; i <task->num_total_tasks; ++i){
                            RunnableChunk* rc = new RunnableChunk();
                            rc->runnable = task->runnable;
                            rc->num_total_tasks = task->num_total_tasks;
                            rc->task_id=i;
                            rc->bulk=task;
                            runnable_queue.push(rc);

                        }
                        running_queue.push_back(task);
                        queue_mutex.unlock();
                        task_available.notify_all(); // ???

                    }
                }
                waiting_queue.remove_if([](QueuedTask* task){return task->queued;});

            }
}


TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {



    std::unique_lock<std::mutex> waiting_lock(waiting_mutex);
    TaskID registered_id = dependency_map.size();
    QueuedTask* new_record = new QueuedTask;
    new_record->runnable = runnable;
    new_record->num_total_tasks = num_total_tasks;
    new_record->tasks_queued = num_total_tasks;
    new_record->deps = deps;
    new_record->completed_tasks = 0;
    new_record->registered_id = registered_id;
    dependency_map[registered_id] = new_record;
    if(deps.empty()){
    std::unique_lock<std::mutex> queue_lock(queue_mutex);
    running_queue.push_back(new_record);

    for(int i =0; i <new_record->num_total_tasks; ++i){
            RunnableChunk* rc = new RunnableChunk();
            rc->runnable = new_record->runnable;
            rc->num_total_tasks = new_record->num_total_tasks;
            rc->task_id=i;
            rc->bulk=new_record;
            runnable_queue.push(rc);
    }
    queue_lock.unlock();
    task_available.notify_all();

    } else {
        waiting_queue.push_back(new_record);
    }
    waiting_lock.unlock();
    queued_runnables++;
    return registered_id;

}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> lock(end_m);
    while(completed_runnables!=queued_runnables){
        // std::cout << "Cannot sync. Waiting ize: " << waiting_queue.size() << " Runnable queue: " << runnable_queue.size() <<  std::endl;
        // std::cout << "Cannot sync. Wcompleted_runnables " << completed_runnables << " queued_runnables " <<queued_runnables <<  std::endl;

        task_graph_changed.wait(lock);
    }
    // std::cout<< "Done!!!!" << std::endl;

}
