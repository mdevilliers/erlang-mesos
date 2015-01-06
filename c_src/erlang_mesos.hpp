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



