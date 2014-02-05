#include <stdio.h>
#include "erl_nif.h"
#include "example.hpp"
#include "mesos_c_api.hpp"    

#define MAXBUFLEN 1024

static int
load(ErlNifEnv* env, void** priv, ERL_NIF_TERM load_info)
{
	state_ptr state = (state_ptr) enif_alloc(sizeof(struct state_t));
    *priv = (void*) state;
    return 0;
}

static void
unload(ErlNifEnv* env, void* priv)
{
	state_ptr state = (state_ptr) priv;
    enif_free(state);
}

static ERL_NIF_TERM
nif_scheduler_init(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
	ErlNifBinary frameworkInfo_binary;
	char masterUrl[MAXBUFLEN];
	CFrameworkInfo info;

	state_ptr state = (state_ptr) enif_priv_data(env);

 	if (enif_inspect_binary(env, argv[0], &frameworkInfo_binary)) 
	{
		ProtobufObj obj ;
		obj.data = frameworkInfo_binary.data;
		obj.size = frameworkInfo_binary.size;
		info =  newFrameworkInfo(obj);
	}else
	{
		return enif_make_tuple2(env, 
								enif_make_atom(env, "argument_error"), 
								enif_make_string(env, "Invalid or corrupted FrameWorkInfo", ERL_NIF_LATIN1));
	}

	if(enif_get_string(env, argv[1], masterUrl , MAXBUFLEN, ERL_NIF_LATIN1 ))
	{	
		state->scheduler_state = scheduler_init(info, masterUrl);		
	}else
	{
		return enif_make_tuple2(env, 
								enif_make_atom(env, "argument_error"), 
								enif_make_string(env, "Invalid or corrupted master url", ERL_NIF_LATIN1));
	}

	return enif_make_atom(env, "ok");
}

static void* scheduler_do_start(void* obj)
{
	state_ptr state = (state_ptr) obj;
	scheduler_start( state->scheduler_state );
	return NULL;
}

static ERL_NIF_TERM
nif_scheduler_start(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
	state_ptr state = (state_ptr) enif_priv_data(env);
	
	if(state->scheduler_state.scheduler == NULL || state->scheduler_state.driver == NULL)
	{
		return enif_make_tuple2(env, 
			enif_make_atom(env, "state_error"), 
			enif_make_string(env, "Scheduler has not been initiated. Call scheduler_init", ERL_NIF_LATIN1));
	}
	
	state->scheduler_worker_thread_options = enif_thread_opts_create("scheduler_thread_opts");
	enif_thread_create("scheduler_thread", 
		&(state->scheduler_worker_thread), 
		scheduler_do_start, 
		state, 
		state->scheduler_worker_thread_options);

	return enif_make_atom(env, "ok");
}

static ErlNifFunc nif_funcs[] = {
	{"scheduler_init", 2 , nif_scheduler_init},
	{"scheduler_start", 0 , nif_scheduler_start}
};

ERL_NIF_INIT(erlang_mesos, nif_funcs, load, NULL, NULL, unload);
