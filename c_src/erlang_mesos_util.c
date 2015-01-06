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


#include "erl_nif.h"

//helper method to turn status into an erlang atom
ERL_NIF_TERM get_atom_from_status(ErlNifEnv* env, int status)
{
    if(status == 1) //DRIVER_NOT_STARTED
    {   
        return enif_make_atom(env, "driver_not_started");

    }else if(status == 2) // DRIVER_RUNNING
    {
        return enif_make_atom(env, "driver_running");

    }else if(status == 3) // DRIVER_ABORTED
    {
        return enif_make_atom(env, "driver_aborted");

    }else if(status == 4) //DRIVER_STOPPED
    {
        return enif_make_atom(env, "driver_stopped");
        
    }else{
        return enif_make_atom(env, "unknown");
    }
}

//helper method to return status to erlang
ERL_NIF_TERM get_return_value_from_status(ErlNifEnv* env, int status)
{
    if(status == 2) // DRIVER_RUNNING
    {
        return enif_make_tuple2(env, 
                            enif_make_atom(env, "ok"), 
                            get_atom_from_status(env, status));
    }else
    {
        return enif_make_tuple2(env, 
                            enif_make_atom(env, "error"), 
                            get_atom_from_status(env, status));
    }
}

//helper method to process an array of binary objects 
int inspect_array_of_binary_objects(ErlNifEnv* env, ERL_NIF_TERM term, ErlNifBinary * binary_arr )
{
    ERL_NIF_TERM head, tail;
    tail = term;

    int i = 0;
    while(enif_get_list_cell(env, tail, &head, &tail))
    {
        ErlNifBinary request_binary;
        if(!enif_inspect_binary(env, head, &request_binary)) 
        {
            return 0;
        }
        binary_arr[i++] = request_binary;
    }
    return 1;
}

// helper method to make an argument error object
ERL_NIF_TERM make_argument_error(ErlNifEnv* env, const char* reason, char* invalid_parameter)
{
   return enif_make_tuple2(env, 
                            enif_make_atom(env, "error"), 
                            enif_make_tuple2(env,
                                            enif_make_atom(env, reason),
                                            enif_make_atom(env, invalid_parameter))
                            );
}