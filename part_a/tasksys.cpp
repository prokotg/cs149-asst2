#include "tasksys.h"
#include <mutex>
#include <thread>
#include <ostream>
#include <iostream>
#include<atomic>
#include <shared_mutex>
#include <chrono>
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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->num_threads = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    int task_queued = 0;
    std::mutex m;

    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    auto streaming = [&]() {
        while(true){
            m.lock();
            if (task_queued != num_total_tasks){
                int my_task = task_queued;
                task_queued++;
                m.unlock();
                runnable->runTask(my_task, num_total_tasks);
            }
            else {
                m.unlock();
                break;
            }

        }
    };

    std::thread pool[num_threads];

    for (int i = 0; i < this->num_threads; i++) {
        pool[i] =  std::thread(streaming);
    }

    for (int i = 0; i < this->num_threads; i++) {
        pool[i].join();
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
    this->num_threads = num_threads;



}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    this->pool_active = false;
    for (int i = 0; i < this->num_threads; i++) {
            pool[i].join();
    }


}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    this->cur_runnable = runnable;
    task_queued = num_total_tasks;
    std::atomic<int> tasks_completed(0);
    this->pool_active=true;
    if(!this->pool){
        // std::cout << "Running " << my_task << std::endl;

        auto streaming = [&]() {
            while(this->pool_active){
                m.lock();
                if (task_queued > 0 ){


                    int my_task = num_total_tasks - task_queued;
                    // std::cout << "Taking " << my_task << std::endl;
                    task_queued--;
                    m.unlock();
                    // std::cout << "Running " << my_task << std::endl;
                    this->cur_runnable->runTask(my_task, num_total_tasks);
                    tasks_completed++;
                }
                else {
                    m.unlock();
                }

            }
        };
        pool = new std::thread[num_threads];

        for (int i = 0; i < this->num_threads; i++) {
            pool[i] =  std::thread(streaming);
        }
    }



    while(true){
        // std::cout << "Tasks completed " << tasks_completed << "Tasks to complete " << num_total_tasks << std::endl;
        if(tasks_completed == num_total_tasks){
            break;
        }
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
    this->num_threads = num_threads;

}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    //TODO!!! proper thread pool cleaning. Use is_pool_alive and ensure every thread released lock on mutex
    // std::cout << "Destructor called" << std::endl;
    m.lock();
    this->pool_active = false;
    m.unlock();
    task_available.notify_all();

    // std::cout << "Workers notified" << std::endl;


    for (int i = 0; i < this->num_threads; i++) {
            pool[i].join();
    }

    // for (int i = 0; i < this->num_threads; i++) {
    //         delete (pool +i);
    // }

}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    std::atomic<int> tasks_completed(0);
    this->pool_active=true;
    // std::cout << "fdsfds" << this->pool;
    task_queued = num_total_tasks;
    if(!this->pool){

        auto streaming = [&](int threadId) {
            while(true){
                std::unique_lock<std::mutex> lck(m);
                if(!this->pool_active){
                    return;
                }
                // std::cout << "threadId" << threadId << " Got lock " <<std::endl;

                if (task_queued > 0 ){
                    int my_task = num_total_tasks - task_queued;
                    // std::cout << "threadId" << threadId << " Running " << my_task << std::endl;

                    task_queued--;
                    // std::cout << "threadId" << threadId << " task_queued is now " << task_queued <<std::endl;

                    // std::cout << "threadId" << threadId << " Released lock " <<std::endl;

                    lck.unlock();
                    runnable->runTask(my_task, num_total_tasks);
                    // std::cout << "threadId" << threadId << " Finished  " << my_task <<std::endl;

                    // std::cout << "Finished " << my_task << std::endl;
                    tasks_completed++;
                }
                else {
                    // if(tasks_completed == num_total_tasks){
                    //     runnable_completed.notify_one();
                    // }
                    // std::cout << "threadId" << threadId << " No job. Sleeping " <<std::endl;
                    // if(!this->pool_active){

                    //     return;
                    // }
                    task_available.wait_for( lck, std::chrono::seconds(2) );

                    // task_available.wait(lck, std::);
                //         if(!this->pool_active){
                //     return;
                // }
                    // std::cout << "Oi mate " << threadId << " has been awaken " <<std::endl;

                }

            }
            // std::cout << "Oi mate " << threadId << " sees pool inactive " <<std::endl;
            return;
        };
        pool = new std::thread[num_threads];

        for (int i = 0; i < this->num_threads; i++) {
            // std::cout << "Starting " << i << std::endl;

            pool[i] =  std::thread(streaming, i);
        }
    }
    // std::unique_lock<std::mutex> lk_c(runnable_m);
    task_available.notify_all();


    // m.unlock();
    while(true){
        // std::cout << "Tasks completed " << tasks_completed << "Tasks to complete " << num_total_tasks << std::endl;
        if(tasks_completed == num_total_tasks){
            // std::cout << "Done!" << std::endl;
            return;
        }
        // runnable_completed.wait(lk_c);

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
