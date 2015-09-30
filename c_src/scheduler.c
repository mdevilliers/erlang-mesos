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


#include <stdio.h>
#include "erl_nif.h"
#include "erlang_mesos_util.c"
#include "erlang_mesos.hpp" 
#include "scheduler_c_api.hpp"    

#define MAXBUFLEN 1024

static int
scheduler_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM load_info)
{
    state_ptr state = (state_ptr) enif_alloc(sizeof(struct state_t));
    state->initilised = 0;
    *priv = (void*) state;
    return 0;
}

static void
scheduler_unload(ErlNifEnv* env, void* priv)
{
    state_ptr state = (state_ptr) priv;
    enif_free(state);
}

static int 
scheduler_upgrade(ErlNifEnv* env, void** priv, void** old_priv_data, ERL_NIF_TERM load_info)
{
    return scheduler_load(env, priv, load_info);
}

static ERL_NIF_TERM
nif_scheduler_init(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary frameworkInfo_binary;
    ErlNifBinary credentials_binary;
    char masterUrl[MAXBUFLEN];
    int implicitAcknowledgements = 1 ;

    state_ptr state = (state_ptr) enif_priv_data(env);
    
    if(state->initilised == 1) 
    {
        return enif_make_tuple2(env, 
            enif_make_atom(env, "error"), 
            enif_make_atom(env, "scheduler_already_inited"));
    }

    ErlNifPid* pid = (ErlNifPid*) enif_alloc(sizeof(ErlNifPid));

    if(!enif_get_local_pid(env, argv[0], pid))
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "pid");
    }

    if (!enif_inspect_binary(env, argv[1], &frameworkInfo_binary)) 
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "framework_info");
    }

    if(!enif_get_string(env, argv[2], masterUrl , MAXBUFLEN, ERL_NIF_LATIN1 ))
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "master_info");   
    }
    
    if(!enif_get_int(env, argv[3], &implicitAcknowledgements))
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "implicit_acknowledgements");   
    }

    if(argc == 5 )
    {
        if(!enif_inspect_binary(env,argv[4], &credentials_binary))
        {       
            return make_argument_error(env, "invalid_or_corrupted_parameter", "credential");    
        }
        state->scheduler_state = scheduler_init(pid, &frameworkInfo_binary, masterUrl, implicitAcknowledgements, 1, &credentials_binary);
    }
    else
    {
        state->scheduler_state = scheduler_init(pid, &frameworkInfo_binary, masterUrl, implicitAcknowledgements, 0, &credentials_binary);
    }
    state->initilised = 1;
    return enif_make_atom(env, "ok");
}

static ERL_NIF_TERM
nif_scheduler_start(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    state_ptr state = (state_ptr) enif_priv_data(env);
    
    if(state->initilised == 0 ) 
    {
        return enif_make_tuple2(env, 
            enif_make_atom(env, "error"), 
            enif_make_atom(env, "scheduler_not_inited"));
    }
    
    SchedulerDriverStatus status = scheduler_start( state->scheduler_state );

    return get_return_value_from_status(env, status);
}

static ERL_NIF_TERM
nif_scheduler_join(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    state_ptr state = (state_ptr) enif_priv_data(env);

    if(state->initilised == 0 ) 
    {
        return enif_make_tuple2(env, 
            enif_make_atom(env, "error"), 
            enif_make_atom(env, "scheduler_not_inited"));
    }
    
    SchedulerDriverStatus status = scheduler_join( state->scheduler_state );

    return get_return_value_from_status(env, status);
}

static ERL_NIF_TERM
nif_scheduler_abort(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    state_ptr state = (state_ptr) enif_priv_data(env);
    
    if(state->initilised == 0 ) 
    {
        return enif_make_tuple2(env, 
            enif_make_atom(env, "error"), 
            enif_make_atom(env, "scheduler_not_inited"));
    }
    
    SchedulerDriverStatus status = scheduler_abort( state->scheduler_state );
    
    if(status == 3){ // DRIVER_ABORTED
        return enif_make_tuple2(env, 
                            enif_make_atom(env, "ok"), 
                            get_atom_from_status(env, status));
    }else{
        return enif_make_tuple2(env, 
                            enif_make_atom(env, "error"), 
                            get_atom_from_status(env, status));
    }
}

static ERL_NIF_TERM
nif_scheduler_stop(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    int failover;
    state_ptr state = (state_ptr) enif_priv_data(env);
    
    if(state->initilised == 0 ) 
    {
        return enif_make_tuple2(env, 
            enif_make_atom(env, "error"), 
            enif_make_atom(env, "sheduler_not_inited"));
    }
    
    if(!enif_get_int( env, argv[0], &failover))
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "failover");
    }

    if(failover < 0 || failover > 1)
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "failover");
    }

    SchedulerDriverStatus status = scheduler_stop( state->scheduler_state, failover );
    
    if(status == 4){ // driver_stopped
        return enif_make_tuple2(env, 
                            enif_make_atom(env, "ok"), 
                            get_atom_from_status(env, status));
    }else{
        return enif_make_tuple2(env, 
                            enif_make_atom(env, "error"), 
                            get_atom_from_status(env, status));
    }
}

static ERL_NIF_TERM
nif_scheduler_acceptOffers(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]){
    unsigned int ids_length ;
    unsigned int ops_length ;

    state_ptr state = (state_ptr) enif_priv_data(env);
    
    if(state->initilised == 0 ) 
    {
        return enif_make_tuple2(env, 
            enif_make_atom(env, "error"), 
            enif_make_atom(env, "scheduler_not_inited"));
    }

    if(!enif_is_list(env, argv[0])) 
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "offerid_array");
    };

    if(!enif_get_list_length(env, argv[0], &ids_length))
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "offerid_array");
    }
    if(!enif_is_list(env, argv[1])) 
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "operations_array");
    };

    if(!enif_get_list_length(env, argv[1], &ops_length))
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "operations_array");
    }

    ErlNifBinary ids_binary_arr[ids_length];
    if(!inspect_array_of_binary_objects(env, argv[0], ids_binary_arr ))
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "offerid_array");
    }
    ErlNifBinary ops_binary_arr[ops_length];
    if(!inspect_array_of_binary_objects(env, argv[1], ops_binary_arr ))
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "operations_array");
    }
    ErlNifBinary filters_binary;
    if (!enif_inspect_binary(env, argv[2], &filters_binary)) 
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "filters");
    }

    BinaryNifArray ids_binaryNifArrayHolder ;
    ids_binaryNifArrayHolder.length = ids_length;
    ids_binaryNifArrayHolder.obj = &ids_binary_arr[0];
    BinaryNifArray ops_binaryNifArrayHolder ;
    ops_binaryNifArrayHolder.length = ops_length;
    ops_binaryNifArrayHolder.obj = &ops_binary_arr[0];

    SchedulerDriverStatus status =  scheduler_acceptOffers(
        state->scheduler_state, &ids_binaryNifArrayHolder, &ops_binaryNifArrayHolder, &filters_binary);
    return get_return_value_from_status(env, status);
}

static ERL_NIF_TERM
nif_scheduler_declineOffer(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]){

    ErlNifBinary offerId_binary;
    ErlNifBinary filters_binary;

    state_ptr state = (state_ptr) enif_priv_data(env);
    
    if(state->initilised == 0 ) 
    {
        return enif_make_tuple2(env, 
            enif_make_atom(env, "error"), 
            enif_make_atom(env, "scheduler_not_inited"));
    }
    if (!enif_inspect_binary(env, argv[0], &offerId_binary)) 
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "offer_id");
    }
    if (!enif_inspect_binary(env, argv[1], &filters_binary)) 
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "filters");
    }

    SchedulerDriverStatus status = scheduler_declineOffer( state->scheduler_state, &offerId_binary, &filters_binary );

    return get_return_value_from_status(env, status);
}

static ERL_NIF_TERM
nif_scheduler_killTask(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]){

    ErlNifBinary taskId_binary;

    state_ptr state = (state_ptr) enif_priv_data(env);
    
    if(state->initilised == 0 ) 
    {
        return enif_make_tuple2(env, 
            enif_make_atom(env, "error"), 
            enif_make_atom(env, "scheduler_not_inited"));
    }
    if (!enif_inspect_binary(env, argv[0], &taskId_binary)) 
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "task_id");
    }

    SchedulerDriverStatus status = scheduler_killTask( state->scheduler_state, &taskId_binary);

    return get_return_value_from_status(env, status);
}

static ERL_NIF_TERM
nif_scheduler_reviveOffers(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    state_ptr state = (state_ptr) enif_priv_data(env);
    
    if(state->initilised == 0 ) 
    {
        return enif_make_tuple2(env, 
            enif_make_atom(env, "error"), 
            enif_make_atom(env, "scheduler_not_inited"));
    }
    
    SchedulerDriverStatus status =  scheduler_reviveOffers( state->scheduler_state );
    return get_return_value_from_status(env, status);
}

static ERL_NIF_TERM
nif_scheduler_sendFrameworkMessage(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]){

    ErlNifBinary executorId_binary;
    ErlNifBinary slaveId_binary;
    char data[MAXBUFLEN];

    state_ptr state = (state_ptr) enif_priv_data(env);
    
    if(state->initilised == 0 ) 
    {
        return enif_make_tuple2(env, 
            enif_make_atom(env, "error"), 
            enif_make_atom(env, "scheduler_not_inited"));
    }
    if (!enif_inspect_binary(env, argv[0], &executorId_binary)) 
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "executor_id");
    }
    if (!enif_inspect_binary(env, argv[1], &slaveId_binary)) 
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "slave_id");
    }
    //REVIEW : buffer length
    if(!enif_get_string(env, argv[2], data , MAXBUFLEN, ERL_NIF_LATIN1 ))
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "data");
    }

    SchedulerDriverStatus status = scheduler_sendFrameworkMessage( state->scheduler_state , 
                                                                        &executorId_binary, 
                                                                        &slaveId_binary, 
                                                                        data);
    return get_return_value_from_status(env, status);
}

static ERL_NIF_TERM
nif_scheduler_requestResources(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    unsigned int length ;

    state_ptr state = (state_ptr) enif_priv_data(env);
    
    if(state->initilised == 0 ) 
    {
        return enif_make_tuple2(env, 
            enif_make_atom(env, "error"), 
            enif_make_atom(env, "scheduler_not_inited"));
    }

    if(!enif_is_list(env, argv[0])) 
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "request_array");
    };

    if(!enif_get_list_length(env, argv[0], &length))
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "request_array");
    }

    ErlNifBinary binary_arr[length];
    if(!inspect_array_of_binary_objects(env, argv[0], binary_arr ))
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "request_array");
    }

    BinaryNifArray binaryNifArrayHolder ;
    binaryNifArrayHolder.length = length;
    binaryNifArrayHolder.obj = &binary_arr[0];

    SchedulerDriverStatus status =  scheduler_requestResources( state->scheduler_state, &binaryNifArrayHolder);
    return get_return_value_from_status(env, status);
}

static ERL_NIF_TERM
nif_scheduler_reconcileTasks(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    unsigned int length ;

    state_ptr state = (state_ptr) enif_priv_data(env);
    
    if(state->initilised == 0 ) 
    {
        return enif_make_tuple2(env, 
            enif_make_atom(env, "error"), 
            enif_make_atom(env, "scheduler_not_inited"));
    }

    if(!enif_is_list(env, argv[0])) 
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "task_status_array");
    };

    if(!enif_get_list_length(env, argv[0], &length))
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "task_status_array");
    }
    
    ErlNifBinary binary_arr[length];
    if(!inspect_array_of_binary_objects(env, argv[0], binary_arr ))
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "task_status_array");
    }   

    BinaryNifArray binaryNifArrayHolder ;
    binaryNifArrayHolder.length = length;
    binaryNifArrayHolder.obj = &binary_arr[0];

    SchedulerDriverStatus status =  scheduler_reconcileTasks( state->scheduler_state, &binaryNifArrayHolder);
    return get_return_value_from_status(env, status);
}

static ERL_NIF_TERM
nif_scheduler_launchTasks(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    unsigned int length ;
    ErlNifBinary offerId_binary;
    ErlNifBinary filters_binary;

    state_ptr state = (state_ptr) enif_priv_data(env);
    
    if(state->initilised == 0 ) 
    {
        return enif_make_tuple2(env, 
            enif_make_atom(env, "error"), 
            enif_make_atom(env, "scheduler_not_inited"));
    }

    if (!enif_inspect_binary(env, argv[0], &offerId_binary)) 
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "offer_id");
    }
    
    if(!enif_is_list(env, argv[1])) 
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "task_info_array");
    };

    if(!enif_get_list_length(env, argv[1], &length))
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "task_info_array");
    }
    
    ErlNifBinary task_info_binary_arr[length];

    if(!inspect_array_of_binary_objects(env, argv[1], task_info_binary_arr ))
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "task_info_array");
    }   

    if (!enif_inspect_binary(env, argv[2], &filters_binary)) 
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "filters");
    }

    BinaryNifArray binaryNifArrayHolder ;
    binaryNifArrayHolder.length = length;
    binaryNifArrayHolder.obj = &task_info_binary_arr[0];

    SchedulerDriverStatus status = scheduler_launchTasks(state->scheduler_state, &offerId_binary, &binaryNifArrayHolder, &filters_binary);
    return get_return_value_from_status(env, status);
}

static ERL_NIF_TERM
nif_scheduler_destroy(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]){

    state_ptr state = (state_ptr) enif_priv_data(env);
    
    if(state->initilised == 0 ) 
    {
        return enif_make_tuple2(env, 
            enif_make_atom(env, "error"), 
            enif_make_atom(env, "scheduler_not_inited"));
    }
    scheduler_destroy(state->scheduler_state);
    state->initilised = 0;
    return enif_make_atom(env, "ok");
}

static ERL_NIF_TERM
nif_scheduler_acknowledgeStatusUpdate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]){

    state_ptr state = (state_ptr) enif_priv_data(env);
    
    ErlNifBinary task_status_binary;

    if(state->initilised == 0 ) 
    {
        return enif_make_tuple2(env, 
            enif_make_atom(env, "error"), 
            enif_make_atom(env, "scheduler_not_inited"));
    }

    if (!enif_inspect_binary(env, argv[0], &task_status_binary)) 
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "task_status");
    }

    SchedulerDriverStatus status = scheduler_acknowledgeStatusUpdate(state->scheduler_state, &task_status_binary);
    return get_return_value_from_status(env, status);
}

static ErlNifFunc nif_funcs[] = {
    {"nif_scheduler_init", 4, nif_scheduler_init},
    {"nif_scheduler_init", 5, nif_scheduler_init},
    {"nif_scheduler_start", 0, nif_scheduler_start},
    {"nif_scheduler_join", 0, nif_scheduler_join},
    {"nif_scheduler_abort", 0, nif_scheduler_abort},
    {"nif_scheduler_stop", 1, nif_scheduler_stop},
    {"nif_scheduler_acceptOffers", 3,nif_scheduler_acceptOffers},
    {"nif_scheduler_declineOffer", 2,nif_scheduler_declineOffer},
    {"nif_scheduler_killTask", 1,nif_scheduler_killTask},
    {"nif_scheduler_reviveOffers", 0 , nif_scheduler_reviveOffers},
    {"nif_scheduler_sendFrameworkMessage", 3, nif_scheduler_sendFrameworkMessage},
    {"nif_scheduler_requestResources", 1, nif_scheduler_requestResources},
    {"nif_scheduler_reconcileTasks", 1,nif_scheduler_reconcileTasks},
    {"nif_scheduler_launchTasks", 3,nif_scheduler_launchTasks},
    {"nif_scheduler_destroy", 0, nif_scheduler_destroy},
    {"nif_scheduler_acknowledgeStatusUpdate", 1, nif_scheduler_acknowledgeStatusUpdate}
};

ERL_NIF_INIT(nif_scheduler, nif_funcs, scheduler_load, NULL, scheduler_upgrade, scheduler_unload);
