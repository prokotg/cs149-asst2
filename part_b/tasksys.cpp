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

    pool = new std::thread[num_threads];


    for (int i = 0; i < this->num_threads; i++) {
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

void TaskSystemParallelThreadPoolSleeping::worker(int threadId){

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

        RunnableChunk * front_task = runnable_queue.front();
        runnable_queue.pop();
        lck.unlock();

        // std::cout << "Executing bulk" << front_task->bulk->registered_id  << " task id " << front_task->task_id << std::endl;

        front_task->runnable->runTask(front_task->task_id, front_task->num_total_tasks);
        front_task->bulk->completed_tasks++;


    }

}


void TaskSystemParallelThreadPoolSleeping::keeper(){
    while(true){
                std::unique_lock<std::mutex> waiting_lock(waiting_mutex);

                // while(waiting_queue.empty() && this->pool_active){
                //     task_with_dependencies.wait(waiting_lock);
                // }

                if(!this->pool_active){
                    // std::cout << "Keeper out!" << std::endl;
                    return;
                }

                // go over running tasks and see if any of it is finished
                std::list<QueuedTask*> local;
                for (auto const& running_task : running_queue){
                    if(running_task->completed_tasks == running_task->num_total_tasks){
                        // std::cout << "Task " << running_task->registered_id << " finished" << std::endl;
                        running_task->finished = true;
                        completed_runnables++;
                        task_graph_changed.notify_all();



                        // find all tasks that wait for this 'running_task' to be finished
                        const TaskID  running_id = running_task->registered_id;
                        queue_mutex.lock();
                        for(TaskID const dependent_id: dependency_map[running_id]){
                            // inform that task with `running_id` has finished by erasing the id from the set of tasks id we wait on
                            QueuedTask* dependent_task = register_map[dependent_id];
                            dependent_task->deps.erase(running_id);
                            // if after erasing, there are no other dependencies, we are good to put that on a runnable queue
                            if(dependent_task->deps.empty()){
                                for(int i =0; i <dependent_task->num_total_tasks; ++i){


                                    RunnableChunk* rc = new RunnableChunk();
                                    rc->runnable = dependent_task->runnable;
                                    rc->num_total_tasks = dependent_task->num_total_tasks;
                                    rc->task_id=i;
                                    rc->bulk=dependent_task;
                                    runnable_queue.push(rc);
                                }
                                local.push_back(dependent_task); // this cannotr be here!
                            }
                        }
                        queue_mutex.unlock();
                        task_available.notify_all();




                        // std::cout << "Bulk id " << task->registered_id << " finished" << std::endl;

                    }
                }

                running_queue.remove_if([](QueuedTask* task){return task->finished;});
                running_queue.splice(running_queue.end(), local); //probably incorrect?

            }
}


TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {



    std::unique_lock<std::mutex> waiting_lock(waiting_mutex);
    TaskID registered_id = register_map.size();
    QueuedTask* new_record = new QueuedTask;
    new_record->runnable = runnable;
    new_record->num_total_tasks = num_total_tasks;
    new_record->tasks_queued = num_total_tasks;
    new_record->deps = std::set<int> (std::make_move_iterator(deps.begin()), std::make_move_iterator(deps.end()));
    new_record->completed_tasks = 0;
    new_record->registered_id = registered_id;
    register_map[registered_id] = new_record;
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
        // add information to the dependency map that when dependency finishes,
        // we must call out on child to see if it can be run
        for(TaskID const &dep: deps){
            dependency_map[dep].insert(registered_id);
        }
        // waiting_queue.push_back(new_record);
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
