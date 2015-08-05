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
#include "executor_c_api.hpp" 

#define MAXBUFLEN 1024

static int
executor_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM load_info)
{
    state_ptr state = (state_ptr) enif_alloc(sizeof(struct state_t));
    state->initilised = 0;
    *priv = (void*) state;
    return 0;
}

static void
executor_unload(ErlNifEnv* env, void* priv)
{
    state_ptr state = (state_ptr) priv;
    enif_free(state);
}

static int 
executor_upgrade(ErlNifEnv* env, void** priv, void** old_priv_data, ERL_NIF_TERM load_info)
{
    return executor_load(env, priv, load_info);
}

static ERL_NIF_TERM
nif_executor_init(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{

    state_ptr state = (state_ptr) enif_priv_data(env);
    
    if(state->initilised == 1) 
    {
        return enif_make_tuple2(env, 
            enif_make_atom(env, "error"), 
            enif_make_atom(env, "executor_already_inited"));
    }

    ErlNifPid* pid = (ErlNifPid*) enif_alloc(sizeof(ErlNifPid));

    if(!enif_get_local_pid(env, argv[0], pid))
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "pid");
    }
    state->executor_state = executor_init(pid);
    
    state->initilised = 1;
    return enif_make_atom(env, "ok");
}

static ERL_NIF_TERM
nif_executor_start(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    state_ptr state = (state_ptr) enif_priv_data(env);
    
    if(state->initilised == 0 ) 
    {
        return enif_make_tuple2(env, 
            enif_make_atom(env, "error"), 
            enif_make_atom(env, "executor_not_inited"));
    }
    
    ExecutorDriverStatus status = executor_start( state->executor_state );

    return get_return_value_from_status(env, status);
}


static ERL_NIF_TERM
nif_executor_abort(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    state_ptr state = (state_ptr) enif_priv_data(env);
    
    if(state->initilised == 0 ) 
    {
        return enif_make_tuple2(env, 
            enif_make_atom(env, "error"), 
            enif_make_atom(env, "executor_not_inited"));
    }
    
    ExecutorDriverStatus status = executor_abort( state->executor_state );
    
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
nif_executor_join(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    state_ptr state = (state_ptr) enif_priv_data(env);

    if(state->initilised == 0 ) 
    {
        return enif_make_tuple2(env, 
            enif_make_atom(env, "error"), 
            enif_make_atom(env, "executor_not_inited"));
    }
    
    ExecutorDriverStatus status = executor_join( state->executor_state );

    return get_return_value_from_status(env, status);
}


static ERL_NIF_TERM
nif_executor_stop(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{

    state_ptr state = (state_ptr) enif_priv_data(env);
    
    if(state->initilised == 0 ) 
    {
        return enif_make_tuple2(env, 
            enif_make_atom(env, "error"), 
            enif_make_atom(env, "executor_not_inited"));
    }

    ExecutorDriverStatus status = executor_stop( state->executor_state );
    
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
nif_executor_sendFrameworkMessage(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]){

    char data[MAXBUFLEN];

    state_ptr state = (state_ptr) enif_priv_data(env);
    
    if(state->initilised == 0 ) 
    {
        return enif_make_tuple2(env, 
            enif_make_atom(env, "error"), 
            enif_make_atom(env, "executor_not_inited"));
    }
    
    //REVIEW : buffer length
    if(!enif_get_string(env, argv[0], data , MAXBUFLEN, ERL_NIF_LATIN1 ))
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "data");
    }

    ExecutorDriverStatus status = executor_sendFrameworkMessage( state->executor_state, 
                                                                        data);
    return get_return_value_from_status(env, status);
}

static ERL_NIF_TERM
nif_executor_sendStatusUpdate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]){

    ErlNifBinary taskStatus_binary;
    state_ptr state = (state_ptr) enif_priv_data(env);
    
    if(state->initilised == 0 ) 
    {
        return enif_make_tuple2(env, 
            enif_make_atom(env, "error"), 
            enif_make_atom(env, "executor_not_inited"));
    }
    
    if (!enif_inspect_binary(env, argv[0], &taskStatus_binary)) 
    {
        return make_argument_error(env, "invalid_or_corrupted_parameter", "task_status");
    }

    ExecutorDriverStatus status = executor_sendStatusUpdate( state->executor_state, 
                                                                    &taskStatus_binary);
    return get_return_value_from_status(env, status);
}

static ERL_NIF_TERM
nif_executor_destroy(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]){

    state_ptr state = (state_ptr) enif_priv_data(env);
    
    if(state->initilised == 0 ) 
    {
        return enif_make_tuple2(env, 
            enif_make_atom(env, "error"), 
            enif_make_atom(env, "executor_not_inited"));
    }
    executor_destroy(state->executor_state);
    state->initilised = 0;
    return enif_make_atom(env, "ok");
}

static ErlNifFunc executor_nif_funcs[] = {
    {"nif_executor_init", 1, nif_executor_init},
    {"nif_executor_start", 0, nif_executor_start},
    {"nif_executor_join", 0, nif_executor_join},
    {"nif_executor_abort", 0, nif_executor_abort},
    {"nif_executor_stop", 0, nif_executor_stop},
    {"nif_executor_sendFrameworkMessage", 1,nif_executor_sendFrameworkMessage},
    {"nif_executor_sendStatusUpdate", 1,nif_executor_sendStatusUpdate},
    {"nif_executor_destroy" , 0, nif_executor_destroy}
    
};

ERL_NIF_INIT(nif_executor, executor_nif_funcs, executor_load, NULL, executor_upgrade, executor_unload);