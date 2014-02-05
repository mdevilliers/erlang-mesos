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
schedular_init(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
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

static ERL_NIF_TERM
schedular_start(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
	state_ptr state = (state_ptr) enif_priv_data(env);
	
	if(state->scheduler_state.scheduler == NULL || state->scheduler_state.driver == NULL)
	{
		return enif_make_tuple2(env, enif_make_atom(env, "state_error"), enif_make_atom(env, "call_schedular_init_first"));
	}

	scheduler_start( state->scheduler_state ); // needs to start in new thread
	return enif_make_atom(env, "ok");
}

static scheduler_do_start(state_ptr state )
{
	scheduler_start( state->scheduler_state );

	// state->opts = enif_thread_opts_create("thread_opts");
    //if(enif_thread_create(
    //        "", &(state->qthread), thr_main, state, state->opts
    //    ) != 0)
   // {
    //    goto error;
    //}

}



//examples
static ERL_NIF_TERM
aaaa(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
	newCFrameworkInfo("aaaa");
	return enif_make_atom(env, "hello");
}

static ERL_NIF_TERM
hello_from_c(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return enif_make_atom(env, "hello");
}

static ERL_NIF_TERM
hello_from_cpp(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return enif_make_int(env, helloWorld());
}

static ErlNifFunc nif_funcs[] = {
	{"schedular_init", 2 , schedular_init},
	{"schedular_start", 0 , schedular_start},
    {"hello_from_c", 0, hello_from_c},
    {"hello_from_cpp", 0, hello_from_cpp},
   {"aaaa", 0, aaaa}
};

ERL_NIF_INIT(erlang_mesos, nif_funcs, load, NULL, NULL, unload);
