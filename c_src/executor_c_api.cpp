// -------------------------------------------------------------------
// Copyright (c) 2015 Mark deVilliers.  All Rights Reserved.
//
// This file is provided to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain
// a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// -------------------------------------------------------------------


#include <stdio.h>
#include <assert.h>

#include "erl_nif.h"

#include "erlang_mesos.hpp"
#include "executor_c_api.hpp"

#include <mesos/mesos.hpp>
#include <mesos/executor.hpp>
#include "mesos/mesos.pb.h"
#include "utils.hpp"

using namespace mesos;
using namespace std;

#define DRIVER_ABORTED 3;

class CExecutor : public Executor {
public:
  CExecutor() {}

  virtual ~CExecutor() {}

  /**
   * Invoked once the executor driver has been able to successfully
   * connect with Mesos. In particular, a scheduler can pass some
   * data to its executors through the FrameworkInfo.ExecutorInfo's
   * data field.
   */
  virtual void registered(
      ExecutorDriver* driver,
      const ExecutorInfo& executorInfo,
      const FrameworkInfo& frameworkInfo,
      const SlaveInfo& slaveInfo);

  /**
   * Invoked when the executor re-registers with a restarted slave.
   */
  virtual void reregistered(
      ExecutorDriver* driver,
      const SlaveInfo& slaveInfo);
  /**
   * Invoked when the executor becomes "disconnected" from the slave
   * (e.g., the slave is being restarted due to an upgrade).
   */
  virtual void disconnected(ExecutorDriver* driver);

  /**
   * Invoked when a task has been launched on this executor (initiated
   * via Scheduler::launchTasks). Note that this task can be realized
   * with a thread, a process, or some simple computation, however, no
   * other callbacks will be invoked on this executor until this
   * callback has returned.
   */
  virtual void launchTask(ExecutorDriver* driver, const TaskInfo& task);

  /**
   * Invoked when a task running within this executor has been killed
   * (via SchedulerDriver::killTask). Note that no status update will
   * be sent on behalf of the executor, the executor is responsible
   * for creating a new TaskStatus (i.e., with TASK_KILLED) and
   * invoking ExecutorDriver::sendStatusUpdate.
   */
  virtual void killTask(ExecutorDriver* driver, const TaskID& taskId);

  /**
   * Invoked when a framework message has arrived for this
   * executor. These messages are best effort; do not expect a
   * framework message to be retransmitted in any reliable fashion.
   */
  virtual void frameworkMessage(ExecutorDriver* driver, const string& data);

  /**
   * Invoked when the executor should terminate all of its currently
   * running tasks. Note that after a Mesos has determined that an
   * executor has terminated any tasks that the executor did not send
   * terminal status updates for (e.g., TASK_KILLED, TASK_FINISHED,
   * TASK_FAILED, etc) a TASK_LOST status update will be created.
   */
  virtual void shutdown(ExecutorDriver* driver);

  /**
   * Invoked when a fatal error has occured with the executor and/or
   * executor driver. The driver will be aborted BEFORE invoking this
   * callback.
   */
  virtual void error(ExecutorDriver* driver, const string& message);

  ErlNifPid* pid;
};

ExecutorPtrPair executor_init(ErlNifPid* pid)
{

    ExecutorPtrPair ret ;
    
    CExecutor* executor = new CExecutor();
    executor->pid = pid;

    MesosExecutorDriver* driver = new MesosExecutorDriver(executor);

    ret.driver = driver;
    ret.executor = executor;
    return ret;
}

ExecutorDriverStatus executor_start(ExecutorPtrPair state)
{
    assert(state.driver != NULL);

    MesosExecutorDriver* driver = reinterpret_cast<MesosExecutorDriver*> (state.driver);
    return driver->start();
}

ExecutorDriverStatus executor_stop(ExecutorPtrPair state)
{
    assert(state.driver != NULL);

    MesosExecutorDriver* driver = reinterpret_cast<MesosExecutorDriver*> (state.driver);
    return driver->stop();
}

ExecutorDriverStatus executor_abort(ExecutorPtrPair state)
{
    assert(state.driver != NULL);

    MesosExecutorDriver* driver = reinterpret_cast<MesosExecutorDriver*> (state.driver);
    return driver->abort();
}

ExecutorDriverStatus executor_join(ExecutorPtrPair state)
{
    assert(state.driver != NULL);

    MesosExecutorDriver* driver = reinterpret_cast<MesosExecutorDriver*> (state.driver);
    return driver->join();
}

ExecutorDriverStatus executor_run(ExecutorPtrPair state)
{
    assert(state.driver != NULL);

    MesosExecutorDriver* driver = reinterpret_cast<MesosExecutorDriver*> (state.driver);
    return driver->run();

}
ExecutorDriverStatus executor_sendFrameworkMessage(ExecutorPtrPair state, const char* data)
{
    assert(state.driver != NULL);
    assert(data != NULL);    

    MesosExecutorDriver* driver = reinterpret_cast<MesosExecutorDriver*> (state.driver);
    return driver->sendFrameworkMessage(data);
}
ExecutorDriverStatus executor_sendStatusUpdate(ExecutorPtrPair state, ErlNifBinary* taskStatus)
{
    assert(state.driver != NULL);
    assert(taskStatus != NULL);    

    TaskStatus taskStatus_pb;

    if(!deserialize<TaskStatus>(taskStatus_pb,taskStatus)) { return DRIVER_ABORTED; };

    MesosExecutorDriver* driver = reinterpret_cast<MesosExecutorDriver*> (state.driver);
    return driver->sendStatusUpdate(taskStatus_pb);
}

void executor_destroy(ExecutorPtrPair state)
{
    assert(state.driver != NULL);
    assert(state.executor != NULL);

    MesosExecutorDriver* driver = reinterpret_cast<MesosExecutorDriver*>(state.driver);
    CExecutor* executor = reinterpret_cast<CExecutor*>(state.executor);

    delete driver;
    delete executor;
}

// callbacks
void CExecutor::registered(ExecutorDriver* driver,
                      const ExecutorInfo& executorInfo,
                      const FrameworkInfo& frameworkInfo,
                      const SlaveInfo& slaveInfo)
{
    assert(this->pid != NULL);

    ErlNifEnv* env = enif_alloc_env();

    ERL_NIF_TERM executorInfo_pb = pb_obj_to_binary(env, executorInfo);
    ERL_NIF_TERM frameworkInfo_pb = pb_obj_to_binary(env, frameworkInfo);
    ERL_NIF_TERM slaveInfo_pb = pb_obj_to_binary(env, slaveInfo);

    ERL_NIF_TERM message = enif_make_tuple4(env, 
                              enif_make_atom(env, "registered"), 
                              executorInfo_pb,
                              frameworkInfo_pb,
                              slaveInfo_pb);
    
    enif_send(NULL, this->pid, env, message);
    enif_clear_env(env);
}

void CExecutor::reregistered(ExecutorDriver* driver,
                      const SlaveInfo& slaveInfo)
{
    assert(this->pid != NULL);

    ErlNifEnv* env = enif_alloc_env();

    ERL_NIF_TERM slaveInfo_pb = pb_obj_to_binary(env, slaveInfo);

    ERL_NIF_TERM message = enif_make_tuple2(env, 
                              enif_make_atom(env, "reregistered"), 
                              slaveInfo_pb);
    
    enif_send(NULL, this->pid, env, message);
    enif_clear_env(env);
}

void CExecutor::disconnected(ExecutorDriver* driver)
{
    assert(this->pid != NULL);

    ErlNifEnv* env = enif_alloc_env();

    ERL_NIF_TERM message = enif_make_tuple(env, 
                              enif_make_atom(env, "disconnected"));
    
    enif_send(NULL, this->pid, env, message);
    enif_clear_env(env);
}

void CExecutor::launchTask(ExecutorDriver* driver, const TaskInfo& task)
{
    assert(this->pid != NULL);

    ErlNifEnv* env = enif_alloc_env();

    ERL_NIF_TERM task_pb = pb_obj_to_binary(env, task);

    ERL_NIF_TERM message = enif_make_tuple2(env, 
                              enif_make_atom(env, "launchTask"), 
                              task_pb);
    
    enif_send(NULL, this->pid, env, message);
    enif_clear_env(env);
}

void CExecutor::killTask(ExecutorDriver* driver, const TaskID& taskId)
{
    assert(this->pid != NULL);

    ErlNifEnv* env = enif_alloc_env();

    ERL_NIF_TERM taskid_pb = pb_obj_to_binary(env, taskId);

    ERL_NIF_TERM message = enif_make_tuple2(env, 
                              enif_make_atom(env, "killTask"), 
                              taskid_pb);
    
    enif_send(NULL, this->pid, env, message);
    enif_clear_env(env);    
}

void CExecutor::frameworkMessage(ExecutorDriver* driver, const string& data)
{
    assert(this->pid != NULL);

    ErlNifEnv* env = enif_alloc_env();

    ERL_NIF_TERM message = enif_make_tuple2(env, 
                              enif_make_atom(env, "frameworkMessage"), 
                               enif_make_string(env, data.c_str(), ERL_NIF_LATIN1));
    
    enif_send(NULL, this->pid, env, message);
    enif_clear_env(env);    
}


void CExecutor::shutdown(ExecutorDriver* driver)
{
    assert(this->pid != NULL);

    ErlNifEnv* env = enif_alloc_env();

    ERL_NIF_TERM message = enif_make_tuple(env, 
                              enif_make_atom(env, "shutdown"));
    
    enif_send(NULL, this->pid, env, message);
    enif_clear_env(env);
}

void CExecutor::error(ExecutorDriver* driver, const string& messageStr)
{
    assert(this->pid != NULL);

    ErlNifEnv* env = enif_alloc_env();

    ERL_NIF_TERM message = enif_make_tuple2(env, 
                              enif_make_atom(env, "error"), 
                              enif_make_string(env, messageStr.c_str(), ERL_NIF_LATIN1));
    
    enif_send(NULL, this->pid, env, message);
    enif_clear_env(env);

}