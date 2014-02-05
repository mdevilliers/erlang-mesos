

#ifndef MESOS_API_C_H
#define MESOS_API_C_H

typedef void* CFrameworkInfo ;

typedef struct {
  void* data;
  size_t size;
} ProtobufObj;

typedef struct {
  void* scheduler;
  void* driver;
} SchedulerPtrPair;

typedef int SchedulerDriverStatus ;

struct state_t
{
	//ErlNifMutex*            lock;
    SchedulerPtrPair scheduler_state;
};

typedef struct state_t* state_ptr;

#ifdef __cplusplus
#include <mesos/mesos.hpp>
extern "C" {
#endif

 CFrameworkInfo newFrameworkInfo(ProtobufObj obj);
 CFrameworkInfo newCFrameworkInfo(char* name);
 void delCFrameworkInfo();

 SchedulerPtrPair scheduler_init(CFrameworkInfo info, const char* master);
 SchedulerDriverStatus scheduler_start(SchedulerPtrPair state);
 SchedulerDriverStatus scheduler_join(SchedulerPtrPair state);
#ifdef __cplusplus
}
#endif
#endif // MESOS_API_C_H
