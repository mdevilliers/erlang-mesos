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


#ifndef MESOS_EXECUTOR_API_C_H
#define MESOS_EXECUTOR_API_C_H

#include "erl_nif.h"
#include "erlang_mesos.hpp"
#ifdef __cplusplus

#include "erl_nif.h"

extern "C" {
#endif

    ExecutorPtrPair executor_init(ErlNifPid* pid);
    ExecutorDriverStatus executor_start(ExecutorPtrPair state);
    ExecutorDriverStatus executor_stop(ExecutorPtrPair state);
    ExecutorDriverStatus executor_abort(ExecutorPtrPair state);
    ExecutorDriverStatus executor_join(ExecutorPtrPair state);
    ExecutorDriverStatus executor_run(ExecutorPtrPair state);
    ExecutorDriverStatus executor_sendFrameworkMessage(ExecutorPtrPair state, const char* data);
    ExecutorDriverStatus executor_sendStatusUpdate(ExecutorPtrPair state, ErlNifBinary* taskStatus);
    void executor_destroy(ExecutorPtrPair state);

#ifdef __cplusplus
}
#endif
#endif // MESOS_EXECUTOR_API_C_H
