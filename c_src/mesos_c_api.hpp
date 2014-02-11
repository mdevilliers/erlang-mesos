

#ifndef MESOS_API_C_H
#define MESOS_API_C_H

#include "erl_nif.h"

typedef struct {
  void* scheduler;
  void* driver;
} SchedulerPtrPair;

typedef int SchedulerDriverStatus ;

struct state_t
{
	//ErlNifMutex*            lock;
    SchedulerPtrPair scheduler_state;
    //ErlNifThreadOpts*   scheduler_worker_thread_options;
    //ErlNifTid* scheduler_worker_thread; 
};

typedef struct state_t* state_ptr;

#ifdef __cplusplus
#include <mesos/mesos.hpp>
extern "C" {
#endif

 SchedulerPtrPair scheduler_init( ErlNifPid* pid, ErlNifBinary* info, const char* master, int credentialssupplied, ErlNifBinary* credentials);
 SchedulerDriverStatus scheduler_start(SchedulerPtrPair state);
 SchedulerDriverStatus scheduler_join(SchedulerPtrPair state);
 SchedulerDriverStatus scheduler_abort(SchedulerPtrPair state);
 SchedulerDriverStatus scheduler_stop(SchedulerPtrPair state, int failover);
 SchedulerDriverStatus declineOffer(SchedulerPtrPair state, ErlNifBinary* offerId, ErlNifBinary* filters);

/*
  virtual Status requestResources(const std::vector<Request>& requests);
  virtual Status launchTasks(const OfferID& offerId,
                             const std::vector<TaskInfo>& tasks,
                             const Filters& filters = Filters());
  virtual Status killTask(const TaskID& taskId);
  virtual Status declineOffer(const OfferID& offerId,
                              const Filters& filters = Filters());
  virtual Status reviveOffers();
  virtual Status sendFrameworkMessage(const ExecutorID& executorId,
                                      const SlaveID& slaveId,
                                      const std::string& data);
  virtual Status reconcileTasks(
      const std::vector<TaskStatus>& statuses);
*/
#ifdef __cplusplus
}
#endif
#endif // MESOS_API_C_H
