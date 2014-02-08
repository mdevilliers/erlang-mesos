

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

#ifdef __cplusplus
}
#endif
#endif // MESOS_API_C_H
