#ifndef MESOS_API_C_H
#define MESOS_API_C_H

#include "erl_nif.h"

typedef void* ExecutorDriverPtr;

typedef struct {
  void* executor;
  void* driver;
} ExecutorPtrPair;

typedef int ExecutorDriverStatus;

typedef struct {
  void* scheduler;
  void* driver;
} SchedulerPtrPair;

typedef int SchedulerDriverStatus ;

struct state_t
{
    int initilised;
    SchedulerPtrPair scheduler_state;
    ExecutorPtrPair executor_state;
};

typedef struct state_t* state_ptr;

typedef struct{
	unsigned int length;
	ErlNifBinary* obj;
} BinaryNifArray;

#endif // MESOS_API_C_H



