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


#ifndef MESOS_SCHEDULER_API_C_H
#define MESOS_SCHEDULER_API_C_H

#include "erl_nif.h"

#ifdef __cplusplus
extern "C" {
#endif

  SchedulerPtrPair scheduler_init(ErlNifPid* pid, ErlNifBinary* info, const char* master, int implicitAcknoledgements, int credentialssupplied, ErlNifBinary* credentials);
  SchedulerDriverStatus scheduler_start(SchedulerPtrPair state);
  SchedulerDriverStatus scheduler_join(SchedulerPtrPair state);
  SchedulerDriverStatus scheduler_abort(SchedulerPtrPair state);
  SchedulerDriverStatus scheduler_stop(SchedulerPtrPair state, int failover);
  SchedulerDriverStatus scheduler_acceptOffers(SchedulerPtrPair state, BinaryNifArray* offerIds, BinaryNifArray* operations, ErlNifBinary* filters);
  SchedulerDriverStatus scheduler_declineOffer(SchedulerPtrPair state, ErlNifBinary* offerId, ErlNifBinary* filters);
  SchedulerDriverStatus scheduler_killTask(SchedulerPtrPair state, ErlNifBinary* taskId);
  SchedulerDriverStatus scheduler_reviveOffers(SchedulerPtrPair state);
  SchedulerDriverStatus scheduler_sendFrameworkMessage(SchedulerPtrPair state, ErlNifBinary* executorId, ErlNifBinary* slaveId, const char* data);
  SchedulerDriverStatus scheduler_requestResources(SchedulerPtrPair state, BinaryNifArray* requests);
  SchedulerDriverStatus scheduler_reconcileTasks(SchedulerPtrPair state, BinaryNifArray* taskStatus);
  SchedulerDriverStatus scheduler_launchTasks(SchedulerPtrPair state, ErlNifBinary* offerId, BinaryNifArray* tasks, ErlNifBinary* filters);
  void scheduler_destroy (SchedulerPtrPair state);
  SchedulerDriverStatus scheduler_acknowledgeStatusUpdate(SchedulerPtrPair state, ErlNifBinary* taskStatus);

#ifdef __cplusplus
}
#endif
#endif // MESOS_SCHEDULER_API_C_H
