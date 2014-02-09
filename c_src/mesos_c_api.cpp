
#include <stdio.h>
#include <assert.h>

#include "erl_nif.h"

#include "mesos_c_api.hpp"

#include <mesos/mesos.hpp>
#include <mesos/scheduler.hpp>
#include "mesos/mesos.pb.h"
#include "utils.hpp"

using namespace mesos;
using namespace std;

class CScheduler : public Scheduler
{
public:
  CScheduler() {}

   ~CScheduler() {}

  /**
   * Invoked when the scheduler successfully registers with a Mesos
   * master. A unique ID (generated by the master) used for
   * distinguishing this framework from others and MasterInfo
   * with the ip and port of the current master are provided as arguments.
   */
   virtual void registered(SchedulerDriver* driver,
                          const FrameworkID& frameworkId,
                          const MasterInfo& masterInfo);


  /**
   * Invoked when the scheduler re-registers with a newly elected Mesos master.
   * This is only called when the scheduler has previously been registered.
   * MasterInfo containing the updated information about the elected master
   * is provided as an argument.
   */
   virtual void reregistered(SchedulerDriver* driver,
                            const MasterInfo& masterInfo);

  /**
   * Invoked when the scheduler becomes "disconnected" from the master
   * (e.g., the master fails and another is taking over).
   */
   virtual void disconnected(SchedulerDriver* driver);

  /**
   * Invoked when resources have been offered to this framework. A
   * single offer will only contain resources from a single slave.
   * Resources associated with an offer will not be re-offered to
   * _this_ framework until either (a) this framework has rejected
   * those resources (see SchedulerDriver::launchTasks) or (b) those
   * resources have been rescinded (see Scheduler::offerRescinded).
   * Note that resources may be concurrently offered to more than one
   * framework at a time (depending on the allocator being used). In
   * that case, the first framework to launch tasks using those
   * resources will be able to use them while the other frameworks
   * will have those resources rescinded (or if a framework has
   * already launched tasks with those resources then those tasks will
   * fail with a TASK_LOST status and a message saying as much).
   */
   virtual void resourceOffers(SchedulerDriver* driver,
                              const std::vector<Offer>& offers);

  /**
   * Invoked when an offer is no longer valid (e.g., the slave was
   * lost or another framework used resources in the offer). If for
   * whatever reason an offer is never rescinded (e.g., dropped
   * message, failing over framework, etc.), a framwork that attempts
   * to launch tasks using an invalid offer will receive TASK_LOST
   * status updates for those tasks (see Scheduler::resourceOffers).
   */
   virtual void offerRescinded(SchedulerDriver* driver,
                              const OfferID& offerId);

  /**
   * Invoked when the status of a task has changed (e.g., a slave is
   * lost and so the task is lost, a task finishes and an executor
   * sends a status update saying so, etc). Note that returning from
   * this callback _acknowledges_ receipt of this status update! If
   * for whatever reason the scheduler aborts during this callback (or
   * the process exits) another status update will be delivered (note,
   * however, that this is currently not true if the slave sending the
   * status update is lost/fails during that time).
   */
   virtual void statusUpdate(SchedulerDriver* driver,
                            const TaskStatus& status);

  /**
   * Invoked when an executor sends a message. These messages are best
   * effort; do not expect a framework message to be retransmitted in
   * any reliable fashion.
   */
   void frameworkMessage(SchedulerDriver* driver,
                                const ExecutorID& executorId,
                                const SlaveID& slaveId,
                                const std::string& data) {
                                };

  /**
   * Invoked when a slave has been determined unreachable (e.g.,
   * machine failure, network partition). Most frameworks will need to
   * reschedule any tasks launched on this slave on a new slave.
   */
   void slaveLost(SchedulerDriver* driver,
                         const SlaveID& slaveId)
                         {
                         } ;

  /**
   * Invoked when an executor has exited/terminated. Note that any
   * tasks running will have TASK_LOST status updates automagically
   * generated.
   */
   void executorLost(SchedulerDriver* driver,
                            const ExecutorID& executorId,
                            const SlaveID& slaveId,
                            int status)
                            {
                            };

  /**
   * Invoked when there is an unrecoverable error in the scheduler or
   * scheduler driver. The driver will be aborted BEFORE invoking this
   * callback.
   */
   void error(SchedulerDriver* driver, const std::string& message)
   {
   };

  void* payload;
  FrameworkInfo info;
  ErlNifPid* pid;
};

SchedulerPtrPair scheduler_init(ErlNifPid* pid, 
                                ErlNifBinary* info, 
                                const char* master, 
                                int credentialssupplied,
                                ErlNifBinary* credentials)
{
    fprintf(stderr, "%s \n" , "scheduler_init" );
    SchedulerPtrPair ret ;
    Credential credentials_pb ;

    CScheduler* scheduler = new CScheduler();
    scheduler->pid = pid;

    deserialize<FrameworkInfo>(scheduler->info,info);
    MesosSchedulerDriver* driver ;

    if(credentialssupplied)
    {

      deserialize<Credential>(credentials_pb,credentials);  

      driver = new MesosSchedulerDriver(
                                       scheduler,
                                       scheduler->info,
                                       std::string(master),
                                       credentials_pb);
    }else
    {
      driver = new MesosSchedulerDriver(
                                     scheduler,
                                     scheduler->info,
                                     std::string(master));
    }

    ret.driver = driver;
    ret.scheduler = scheduler;
    return ret;
}

SchedulerDriverStatus scheduler_start(SchedulerPtrPair state)
{
    assert(state.driver != NULL);

    MesosSchedulerDriver* driver = reinterpret_cast<MesosSchedulerDriver*> (state.driver);
    return driver->start();
}

SchedulerDriverStatus scheduler_join(SchedulerPtrPair state)
{
    assert(state.driver != NULL);

    MesosSchedulerDriver* driver = reinterpret_cast<MesosSchedulerDriver*> (state.driver);
    return driver->join();
}

SchedulerDriverStatus scheduler_abort(SchedulerPtrPair state)
{
    assert(state.driver != NULL);

    MesosSchedulerDriver* driver = reinterpret_cast<MesosSchedulerDriver*> (state.driver);
    return driver->abort();
}

SchedulerDriverStatus scheduler_stop(SchedulerPtrPair state, int failover)
{
    assert(state.driver != NULL);
    MesosSchedulerDriver* driver = reinterpret_cast<MesosSchedulerDriver*> (state.driver);
    if(failover){
      return driver->stop(true);
    }else{
      return driver->stop(false);
    }
}

void CScheduler::registered(SchedulerDriver* driver,
                          const FrameworkID& frameworkId,
                          const MasterInfo& masterInfo)
                          {
    //fprintf(stderr, "%s \n" , "Registered" );
    assert(this->pid != NULL);

    ErlNifEnv* env = enif_alloc_env();

    ERL_NIF_TERM framework_pb = pb_obj_to_binary(env, frameworkId);
    ERL_NIF_TERM masterInfo_pb = pb_obj_to_binary(env, masterInfo);

    ERL_NIF_TERM message = enif_make_tuple3(env, 
                              enif_make_atom(env, "registered"), 
                              framework_pb,
                              masterInfo_pb);
    
    enif_send(NULL, this->pid, env, message);
    //{
    //  fprintf(stderr, "%s \n" , "sent" );  
    //}else
    //{
    //  fprintf(stderr, "%s \n" , "not sent" );
    //}
    
    enif_clear_env(env);
}

void CScheduler::reregistered(SchedulerDriver* driver,
                            const MasterInfo& masterInfo)
                            {
    //fprintf(stderr, "%s \n" , "Reregistered" );
    assert(this->pid != NULL);

    ErlNifEnv* env = enif_alloc_env();

    ERL_NIF_TERM masterInfo_pb = pb_obj_to_binary(env, masterInfo);

    ERL_NIF_TERM message = enif_make_tuple2(env, 
                              enif_make_atom(env, "reregistered"), 
                              masterInfo_pb);
    
   enif_send(NULL, this->pid, env, message);
    //{
    //  fprintf(stderr, "%s \n" , "sent" );  
    //}else
    //{
    //  fprintf(stderr, "%s \n" , "not sent" );
    //}
    
    enif_clear_env(env);
};

void CScheduler::disconnected(SchedulerDriver* driver)
{
    //fprintf(stderr, "%s \n" , "Disconnected" );
    assert(this->pid != NULL);

    ErlNifEnv* env = enif_alloc_env();

    ERL_NIF_TERM message = enif_make_tuple(env, 
                              enif_make_atom(env, "disconnected"));
    
    enif_send(NULL, this->pid, env, message);
    //{
    //  fprintf(stderr, "%s \n" , "sent" );  
    //}else
    //{
    //  fprintf(stderr, "%s \n" , "not sent" );
    //}
    
    enif_clear_env(env);
};

void CScheduler::offerRescinded(SchedulerDriver* driver,
                              const OfferID& offerId)
{
    //fprintf(stderr, "%s \n" , "offerRescinded" );
    assert(this->pid != NULL);

    ErlNifEnv* env = enif_alloc_env();

    ERL_NIF_TERM message = enif_make_tuple2(env, 
                              enif_make_atom(env, "offerRescinded"),
                              pb_obj_to_binary(env, offerId));
    
    enif_send(NULL, this->pid, env, message);
    //{
    //  fprintf(stderr, "%s \n" , "sent" );  
    //}else
    //{
    //  fprintf(stderr, "%s \n" , "not sent" );
    //}
    
    enif_clear_env(env);
} ;

void CScheduler::statusUpdate(SchedulerDriver* driver,
                            const TaskStatus& status){
    //fprintf(stderr, "%s \n" , "statusUpdate" );
    assert(this->pid != NULL);

    ErlNifEnv* env = enif_alloc_env();

    ERL_NIF_TERM message = enif_make_tuple2(env, 
                              enif_make_atom(env, "statusUpdate"),
                              pb_obj_to_binary(env, status));
    
    enif_send(NULL, this->pid, env, message);
    //{
    //  fprintf(stderr, "%s \n" , "sent" );  
    //}else
    //{
    //  fprintf(stderr, "%s \n" , "not sent" );
    //}
    
    enif_clear_env(env);


                            } ;

void CScheduler::resourceOffers(SchedulerDriver* driver,
                              const std::vector<Offer>& offers)
                              {
      fprintf(stderr, "%s \n" , "Offers" );
      for(uint i = 0 ; i < offers.size(); i++)
      {
        Offer offer = offers.at(i);
        fprintf(stderr, "%s \n" , offer.DebugString().c_str() );
      }
} ;