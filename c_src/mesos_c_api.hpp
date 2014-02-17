

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
	  int initilised;
    SchedulerPtrPair scheduler_state;
};

typedef struct state_t* state_ptr;

#ifdef __cplusplus
#include <mesos/mesos.hpp>
extern "C" {
#endif

  SchedulerPtrPair scheduler_init(ErlNifPid* pid, ErlNifBinary* info, const char* master, int credentialssupplied, ErlNifBinary* credentials);
  SchedulerDriverStatus scheduler_start(SchedulerPtrPair state);
  SchedulerDriverStatus scheduler_join(SchedulerPtrPair state);
  SchedulerDriverStatus scheduler_abort(SchedulerPtrPair state);
  SchedulerDriverStatus scheduler_stop(SchedulerPtrPair state, int failover);
  SchedulerDriverStatus scheduler_declineOffer(SchedulerPtrPair state, ErlNifBinary* offerId, ErlNifBinary* filters);
  SchedulerDriverStatus scheduler_killTask(SchedulerPtrPair state, ErlNifBinary* taskId);
  SchedulerDriverStatus scheduler_reviveOffers(SchedulerPtrPair state);
  SchedulerDriverStatus scheduler_sendFrameworkMessage(SchedulerPtrPair state, ErlNifBinary* executorId, ErlNifBinary* slaveId, const char* data);
  SchedulerDriverStatus scheduler_requestResources(SchedulerPtrPair state, ErlNifBinary* request);
/*
  virtual Status requestResources(const std::vector<Request>& requests);
  virtual Status launchTasks(const OfferID& offerId,
                             const std::vector<TaskInfo>& tasks,
                             const Filters& filters = Filters());
  virtual Status reconcileTasks(
      const std::vector<TaskStatus>& statuses);
*/
#ifdef __cplusplus
}
#endif
#endif // MESOS_API_C_H
