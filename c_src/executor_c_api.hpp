
#ifndef MESOS_EXECUTOR_API_C_H
#define MESOS_EXECUTOR_API_C_H

#include "erl_nif.h"
#include "erlang_mesos.hpp"


#ifdef __cplusplus
#include <mesos/mesos.hpp>
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

#ifdef __cplusplus
}
#endif
#endif // MESOS_EXECUTOR_API_C_H
