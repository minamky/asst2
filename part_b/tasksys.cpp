#include "tasksys.h"
#include "iostream"


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


  /* ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

// const char* TaskSystemParallelThreadPoolSleeping::name() {
//     return "Parallel + Thread Pool + Sleep";
// }

// TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
//     //
//     // TODO: CS149 student implementations may decide to perform setup
//     // operations (such as thread pool construction) here.
//     // Implementations are free to add new class member variables
//     // (requiring changes to tasksys.h).
//     //
   
//     this->num_threads = num_threads;
//     this->threads_sleeping = 0;
//     this->num_batches_done = 0;
//     this->next_task_id = 0;
//     this->stop = false;
//     //this->tasksCompleted = 0;

//     for (int i = 0; i < num_threads; i++){
//         threads.push_back(std::thread(&TaskSystemParallelThreadPoolSleeping::workerThread, this));
//     }
// }

// TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
//     //
//     // TODO: CS149 student implementations may decide to perform cleanup
//     // operations (such as thread pool shutdown construction) here.
//     // Implementations are free to add new class member variables
//     // (requiring changes to tasksys.h).
//     //
    
//     std::unique_lock<std::mutex> lock(mtx);
//     stop = true;
//     cv.notify_all();


    
//     lock.unlock();
   
//     for (auto& thread: threads){
//         thread.join();
        
//     }
// }

// void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


//     //
//     // TODO: CS149 students will modify the implementation of this
//     // method in Parts A and B.  The implementation provided below runs all
//     // tasks sequentially on the calling thread.
//     //
    
//     runAsyncWithDeps(runnable, num_total_tasks, {});

//     sync();
    
// }

// TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
//                                                     const std::vector<TaskID>& deps) {

//     //
//     // TODO: CS149 students will implement this method in Part B.
//     //

//     //add all tasks to either ready queue or waiting queue
   
//     std::unique_lock<std::mutex> lock(mtx);
    

//     std::cout << "made it to run async" << std::endl;
//     TaskID task_id = next_task_id;
//     next_task_id++;
//     //mtx.unlock();
//     tasksCompletedPerBatch[task_id] = 0;
//     lock.unlock();

//     if (deps.empty()) {
//         std::cout << "made it inside this if" << std::endl;
//         lock.lock();
//         for (int i = 0; i < num_total_tasks; i++) {
//             ready_queue.push(std::make_tuple(runnable, task_id, i, num_total_tasks));
//         }
//         cv.notify_all();
//         lock.unlock();

//     }
//     else {
//         std::cout << "made it to the else" << std::endl;
//         int unfinished_dependencies = 0;
//         for (TaskID dependency: deps) {
//             if (finishedTasks.find(dependency) == finishedTasks.end()) {
//                 unfinished_dependencies++;
//                 std::unique_lock<std::mutex> depsLock(dependency_mtx);
//                 waiting_map[dependency].push_back(std::make_tuple(runnable, task_id, num_total_tasks));
//                 // depsLock.unlock();
//             }
//         }
//         std::cout << "made it to check the dependencies" << std::endl;
//         if (unfinished_dependencies == 0) {
//             lock.lock();
//             for (int i = 0; i < num_total_tasks; i++) {
//                 ready_queue.push(std::make_tuple(runnable, task_id, i, num_total_tasks));
//             }
//             lock.unlock();
//             cv.notify_all();
    
//         }
//         else {
//             // depsLock.lock();
//             std::unique_lock<std::mutex> depsLock(dependency_mtx);
//             dependency_counter[task_id] = unfinished_dependencies;
//             // depsLock.unlock();
//         }
//     }
//     std::cout << "made it to the end" << std::endl;
//     return task_id;
// }

// void TaskSystemParallelThreadPoolSleeping::workerThread() {
//     std::cout << "made it to workerThread" << std::endl;
//     while (true){
//         std::unique_lock<std::mutex> lock(mtx);

//         while (ready_queue.empty() && !stop){
//             //threads_sleeping++;
//             cv.wait(lock);
//             //threads_sleeping--;
//         }
        
//         if (stop){
//             break;
//         }
//         auto task = ready_queue.front();
//         ready_queue.pop();

//         lock.unlock();
//         IRunnable* cur_runnable = std::get<0>(task);
//         int cur_task = std::get<2>(task);
//         int total_tasks = std::get<3>(task);
//         int taskID = std::get<1>(task);
//         cur_runnable->runTask(cur_task, total_tasks);

//         lock.lock();
//         tasksCompletedPerBatch[taskID] = tasksCompletedPerBatch[taskID] + 1;

//         if (tasksCompletedPerBatch[taskID] == total_tasks) {
//             updateDependency_Queue(taskID);
//         }
//         lock.unlock();
//     }
// }

// // helper function that moves tasks from waiting ready queue after a dependency completes
// void TaskSystemParallelThreadPoolSleeping::updateDependency_Queue(TaskID completed_task) {
//     std::cout << "made it to update dependency" << std::endl;
//     std::unique_lock<std::mutex> lock(mtx);  // Lock mtx first for shared data
//     std::unique_lock<std::mutex> depsLock(dependency_mtx);  // Then lock dependency_mtx

//     std::cout << "was able to acquire lock" << std::endl;
//     finishedTasks.insert(completed_task);

//     num_batches_done = num_batches_done + 1;
//     auto it = waiting_map.find(completed_task);
//     if (it != waiting_map.end()) {
//         auto waitingTasks = it->second;
//         //waiting_map.erase(it);
//         if (it != waiting_map.end()) {
//             std::cout << "getting here" << std::endl;
//             auto waitingTasks = waiting_map[completed_task];
//             for (auto taskTuple: waitingTasks) {
//                 TaskID waitingTaskID = std::get<1>(taskTuple);
//                 dependency_counter[waitingTaskID] = dependency_counter[waitingTaskID] - 1;
//                 if (dependency_counter[waitingTaskID] == 0) {
//                     IRunnable *current = std::get<0>(taskTuple);
//                     int total_tasks = std::get<2>(taskTuple);
//                     for (int i = 0; i < total_tasks; i++) {
//                         ready_queue.push(std::make_tuple(current, waitingTaskID, i, total_tasks));
//                     }
//                     cv.notify_all();
//                 }                                 
//             }
//             //dependency_counter.erase(waitingTaskID);
//         }
//     }
    

//     if (num_batches_done >= next_task_id) {
//         cv_finished.notify_all();
//     }
// }


// void TaskSystemParallelThreadPoolSleeping::sync() {

//     //
//     // returns when all task batches are complete
//     //

//     std::cout << "made it inside sync" << std::endl;

//     std::unique_lock<std::mutex> lock(mtx);
//     std::cout << "deadlocking here" << std::endl;

//     while(num_batches_done < next_task_id){
//         std::cout << "stuck in while loop" << std::endl;
//         cv_finished.wait(lock);
//         std::cout << "after the wait" << std::endl;
//     }
// }

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
   this->cur_runnable = NULL;
   this->num_threads = num_threads;
   //this->threads_sleeping = 0;
   this->num_batches_done = 0;
   this->next_task_id = 0;
   this->stop = false;

   for (int i = 0; i < num_threads; i++){
       threads.push_back(std::thread(&TaskSystemParallelThreadPoolSleeping::workerThread, this));
   }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
   
   mtx.lock();
   stop = true;
   mtx.unlock();

   cv.notify_all();
  
   for (auto& thread: threads){
       thread.join();
   }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
   runAsyncWithDeps(runnable, num_total_tasks, {});
   sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                   const std::vector<TaskID>& deps) {
   TaskID task_id;
   {
       std::unique_lock<std::mutex> lock(mtx);
       task_id = next_task_id++;
       tasksCompletedPerBatch[task_id] = 0;
   }

   if (deps.empty()) {
       std::unique_lock<std::mutex> lock(mtx);
       for (int i = 0; i < num_total_tasks; i++) {
           ready_queue.push(std::make_tuple(runnable, task_id, i, num_total_tasks));
       }
       cv.notify_all();
   }
   else {
       int unfinished_dependencies = 0;
       {
           std::unique_lock<std::mutex> depsLock(dependency_mtx);
           for (TaskID dependency: deps) {
               if (finishedTasks.find(dependency) == finishedTasks.end()) {
                   unfinished_dependencies++;
                   waiting_map[dependency].push_back(std::make_tuple(runnable, task_id, num_total_tasks));
               }
           }
           if (unfinished_dependencies > 0) {
               dependency_counter[task_id] = unfinished_dependencies;
           }
       }

       if (unfinished_dependencies == 0) {
           std::unique_lock<std::mutex> lock(mtx);
           for (int i = 0; i < num_total_tasks; i++) {
               ready_queue.push(std::make_tuple(runnable, task_id, i, num_total_tasks));
           }
           cv.notify_all();
       }
   }
   return task_id;
}

void TaskSystemParallelThreadPoolSleeping::workerThread() {
   while (true) {
       std::unique_lock<std::mutex> lock(mtx);
       
       while (ready_queue.empty() && !stop) {
           cv.wait(lock);
       }
       
       if (stop) {
           lock.unlock();
           break;
       }

       auto task = ready_queue.front();
       ready_queue.pop();
       lock.unlock();
       
       IRunnable* cur_runnable = std::get<0>(task);
       int cur_task = std::get<2>(task);
       int total_tasks = std::get<3>(task);
       int taskID = std::get<1>(task);
       cur_runnable->runTask(cur_task, total_tasks);
       
       lock.lock();
       tasksCompletedPerBatch[taskID]++;
       if (tasksCompletedPerBatch[taskID] == total_tasks) {
           lock.unlock();
           updateDependency_Queue(taskID);
       } else {
           lock.unlock();
       }
   }
}

void TaskSystemParallelThreadPoolSleeping::updateDependency_Queue(TaskID completed_task) {
   std::vector<std::tuple<IRunnable*, TaskID, int>> tasksToSchedule;
   
       //consider doing one lock for each task id - try to reduce number of time you keep lock while keeping the implementation correct
       //also look at improvements for single threaded version
       std::unique_lock<std::mutex> depsLock(dependency_mtx);
       finishedTasks.insert(completed_task);
       
       
       auto it = waiting_map.find(completed_task);
       depsLock.unlock();
       
       if (it != waiting_map.end()) {
               for (auto taskTuple: it->second) {
                   TaskID waitingTaskID = std::get<1>(taskTuple);
                   
                   std::unique_lock<std::mutex> waiting_task_lock(waiting_map_locks[waitingTaskID]); // when decrementing, obtain lock
                   dependency_counter[waitingTaskID]--;
               
                   if (dependency_counter[waitingTaskID] == 0) { 
                       tasksToSchedule.push_back(taskTuple);
                       //dependency_counter.erase(waitingTaskID);
                   }
                   waiting_task_lock.unlock();
                   
               }
               
               depsLock.lock();
               //waiting_map.erase(it);
               depsLock.unlock();
       }
       
   
   {
       std::unique_lock<std::mutex> lock(mtx);
       num_batches_done++;
       
       for (const auto& taskTuple : tasksToSchedule) {
           IRunnable* current = std::get<0>(taskTuple);
           TaskID waitingTaskID = std::get<1>(taskTuple);
           int total_tasks = std::get<2>(taskTuple);
           
           for (int i = 0; i < total_tasks; i++) {
               ready_queue.push(std::make_tuple(current, waitingTaskID, i, total_tasks));
           }
       }
       
       if (!tasksToSchedule.empty()) {
           cv.notify_all();
       }
       
       if (num_batches_done >= next_task_id) {
           cv_finished.notify_all();
       }
   }
}

void TaskSystemParallelThreadPoolSleeping::sync() {
   std::unique_lock<std::mutex> lock(mtx);
   while(num_batches_done < next_task_id) {
       cv_finished.wait(lock);
   }
}

//release lock and then call notify - optimize code
