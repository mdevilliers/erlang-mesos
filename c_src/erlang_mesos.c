#include <stdio.h>
#include "erl_nif.h"
#include "mesos_c_api.hpp"    

#define MAXBUFLEN 1024

static int
load(ErlNifEnv* env, void** priv, ERL_NIF_TERM load_info)
{
	state_ptr state = (state_ptr) enif_alloc(sizeof(struct state_t));
	state->initilised = 0;
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
	ErlNifBinary credentials_binary;
	char masterUrl[MAXBUFLEN];

	state_ptr state = (state_ptr) enif_priv_data(env);
	
	ErlNifPid* pid = (ErlNifPid*) enif_alloc(sizeof(ErlNifPid));

	if(!enif_get_local_pid(env, argv[0], pid))
    {
    	return enif_make_tuple2(env, 
								enif_make_atom(env, "argument_error"), 
								enif_make_string(env, "Invalid or corrupted Pid", ERL_NIF_LATIN1));
    }

 	if (!enif_inspect_binary(env, argv[1], &frameworkInfo_binary)) 
	{
		return enif_make_tuple2(env, 
					enif_make_atom(env, "argument_error"), 
					enif_make_string(env, "Invalid or corrupted FrameWorkInfo", ERL_NIF_LATIN1));
	}

	if(!enif_get_string(env, argv[2], masterUrl , MAXBUFLEN, ERL_NIF_LATIN1 ))
	{
		return enif_make_tuple2(env, 
					enif_make_atom(env, "argument_error"), 
					enif_make_string(env, "Invalid or corrupted master url", ERL_NIF_LATIN1));
	}

	if(argc == 4 )
	{
		if(!enif_inspect_binary(env,argv[3], &credentials_binary))
		{			
			return enif_make_tuple2(env, 
						enif_make_atom(env, "argument_error"), 
						enif_make_string(env, "Invalid or corrupted Credential", ERL_NIF_LATIN1));
			
		}
		state->scheduler_state = scheduler_init(pid, &frameworkInfo_binary, masterUrl, 1, &credentials_binary);
	}
	else
	{
		state->scheduler_state = scheduler_init(pid, &frameworkInfo_binary, masterUrl, 0, &credentials_binary);
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
			enif_make_atom(env, "state_error"), 
			enif_make_string(env, "Scheduler has not been initiated. Call scheduler_init first.", ERL_NIF_LATIN1));
	}
	
	SchedulerDriverStatus status = scheduler_start( state->scheduler_state );

	return enif_make_tuple2(env, 
							enif_make_atom(env, "ok"), 
							enif_make_int(env, status));
}

static ERL_NIF_TERM
nif_scheduler_join(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
	state_ptr state = (state_ptr) enif_priv_data(env);

	if(state->initilised == 0 ) 
	{
		return enif_make_tuple2(env, 
			enif_make_atom(env, "state_error"), 
			enif_make_string(env, "Scheduler has not been initiated. Call scheduler_init first.", ERL_NIF_LATIN1));
	}
	
	SchedulerDriverStatus status =  scheduler_join( state->scheduler_state );
	return enif_make_tuple2(env, 
							enif_make_atom(env, "ok"), 
							enif_make_int(env, status));
}



static ERL_NIF_TERM
nif_scheduler_abort(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
	state_ptr state = (state_ptr) enif_priv_data(env);
	
	if(state->initilised == 0 ) 
	{
		return enif_make_tuple2(env, 
			enif_make_atom(env, "state_error"), 
			enif_make_string(env, "Scheduler has not been initiated. Call scheduler_init first.", ERL_NIF_LATIN1));
	}
	
	SchedulerDriverStatus status =  scheduler_abort( state->scheduler_state );
	return enif_make_tuple2(env, 
							enif_make_atom(env, "ok"), 
							enif_make_int(env, status));
}

static ERL_NIF_TERM
nif_scheduler_stop(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
	int failover;
	state_ptr state = (state_ptr) enif_priv_data(env);
	
	if(state->initilised == 0 ) 
	{
		return enif_make_tuple2(env, 
			enif_make_atom(env, "state_error"), 
			enif_make_string(env, "Scheduler has not been initiated. Call scheduler_init first.", ERL_NIF_LATIN1));
	}
	
	if(!enif_get_int( env, argv[0], &failover))
	{
		return enif_make_tuple2(env, 
					enif_make_atom(env, "argument_error"), 
					enif_make_string(env, "Invalid or corrupted failover argument - ensure it is an int e.g. 1 == true, 0 == false", ERL_NIF_LATIN1));
	}

	if(failover < 0 || failover > 1)
	{
		return enif_make_tuple2(env, 
					enif_make_atom(env, "argument_error"), 
					enif_make_string(env, "Invalid or corrupted failover argument - ensure it is an int e.g. 1 == true, 0 == false", ERL_NIF_LATIN1));
	}

	SchedulerDriverStatus status = scheduler_stop( state->scheduler_state, failover );
	
	return enif_make_tuple2(env, 
							enif_make_atom(env, "ok"), 
							enif_make_int(env, status));
}

static ERL_NIF_TERM
nif_scheduler_declineOffer(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]){

	ErlNifBinary offerId_binary;
	ErlNifBinary filters_binary;

	state_ptr state = (state_ptr) enif_priv_data(env);
	
	if(state->initilised == 0 ) 
	{
		return enif_make_tuple2(env, 
			enif_make_atom(env, "state_error"), 
			enif_make_string(env, "Scheduler has not been initiated. Call scheduler_init first.", ERL_NIF_LATIN1));
	}
	if (!enif_inspect_binary(env, argv[0], &offerId_binary)) 
	{
		return enif_make_tuple2(env, 
					enif_make_atom(env, "argument_error"), 
					enif_make_string(env, "Invalid or corrupted OfferID", ERL_NIF_LATIN1));
	}
	if (!enif_inspect_binary(env, argv[1], &filters_binary)) 
	{
		return enif_make_tuple2(env, 
					enif_make_atom(env, "argument_error"), 
					enif_make_string(env, "Invalid or corrupted Filters", ERL_NIF_LATIN1));
	}

	SchedulerDriverStatus status = scheduler_declineOffer( state->scheduler_state, &offerId_binary, &filters_binary );

	return enif_make_tuple2(env, 
							enif_make_atom(env, "ok"), 
							enif_make_int(env, status));
}

static ERL_NIF_TERM
nif_scheduler_killTask(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]){

	ErlNifBinary taskId_binary;

	state_ptr state = (state_ptr) enif_priv_data(env);
	
	if(state->initilised == 0 ) 
	{
		return enif_make_tuple2(env, 
			enif_make_atom(env, "state_error"), 
			enif_make_string(env, "Scheduler has not been initiated. Call scheduler_init first.", ERL_NIF_LATIN1));
	}
	if (!enif_inspect_binary(env, argv[0], &taskId_binary)) 
	{
		return enif_make_tuple2(env, 
					enif_make_atom(env, "argument_error"), 
					enif_make_string(env, "Invalid or corrupted TaskID", ERL_NIF_LATIN1));
	}

	SchedulerDriverStatus status = scheduler_killTask( state->scheduler_state, &taskId_binary);

	return enif_make_tuple2(env, 
							enif_make_atom(env, "ok"), 
							enif_make_int(env, status));
}

static ERL_NIF_TERM
nif_scheduler_reviveOffers(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
	state_ptr state = (state_ptr) enif_priv_data(env);
	
	if(state->initilised == 0 ) 
	{
		return enif_make_tuple2(env, 
			enif_make_atom(env, "state_error"), 
			enif_make_string(env, "Scheduler has not been initiated. Call scheduler_init first.", ERL_NIF_LATIN1));
	}
	
	SchedulerDriverStatus status =  scheduler_reviveOffers( state->scheduler_state );
	return enif_make_tuple2(env, 
							enif_make_atom(env, "ok"), 
							enif_make_int(env, status));
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
			enif_make_atom(env, "state_error"), 
			enif_make_string(env, "Scheduler has not been initiated. Call scheduler_init first.", ERL_NIF_LATIN1));
	}
	if (!enif_inspect_binary(env, argv[0], &executorId_binary)) 
	{
		return enif_make_tuple2(env, 
					enif_make_atom(env, "argument_error"), 
					enif_make_string(env, "Invalid or corrupted ExecutorID", ERL_NIF_LATIN1));
	}
	if (!enif_inspect_binary(env, argv[1], &slaveId_binary)) 
	{
		return enif_make_tuple2(env, 
					enif_make_atom(env, "argument_error"), 
					enif_make_string(env, "Invalid or corrupted SlaveID", ERL_NIF_LATIN1));
	}
	//REVIEW : buffer length
	if(!enif_get_string(env, argv[2], data , MAXBUFLEN, ERL_NIF_LATIN1 ))
	{
		return enif_make_tuple2(env, 
					enif_make_atom(env, "argument_error"), 
					enif_make_string(env, "Invalid or corrupted data parameter", ERL_NIF_LATIN1));
	}

	SchedulerDriverStatus status = scheduler_sendFrameworkMessage( state->scheduler_state , 
					                                                    &executorId_binary, 
					                                                    &slaveId_binary, 
					                                                    data);
	return enif_make_tuple2(env, 
							enif_make_atom(env, "ok"), 
							enif_make_int(env, status));
}

static ErlNifFunc nif_funcs[] = {
	{"scheduler_init", 3, nif_scheduler_init},
	{"scheduler_init", 4, nif_scheduler_init},
	{"scheduler_start", 0, nif_scheduler_start},
	{"scheduler_join", 0, nif_scheduler_join},
	{"scheduler_abort", 0, nif_scheduler_abort},
	{"scheduler_stop", 1, nif_scheduler_stop},
	{"scheduler_declineOffer", 2,nif_scheduler_declineOffer},
	{"scheduler_killTask", 1,nif_scheduler_killTask},
	{"scheduler_reviveOffers", 0 , nif_scheduler_reviveOffers},
	{"scheduler_sendFrameworkMessage", 3, nif_scheduler_sendFrameworkMessage}
};

ERL_NIF_INIT(erlang_mesos, nif_funcs, load, NULL, NULL, unload);
