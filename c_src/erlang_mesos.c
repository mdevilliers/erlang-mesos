#include <stdio.h>
#include "erl_nif.h"
#include "erlang_mesos_util.c"
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
	
	if(state->initilised == 1) 
	{
		return enif_make_tuple2(env, 
			enif_make_atom(env, "state_error"), 
			enif_make_string(env, "Scheduler has not already been initiated. Call scheduler_stop first.", ERL_NIF_LATIN1));
	}

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

	return get_return_value_from_status(env, status);
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

	return get_return_value_from_status(env, status);
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
	
	if(status == 4){ // driver_stopped
		state->initilised = 0;
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

	return get_return_value_from_status(env, status);
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

	return get_return_value_from_status(env, status);
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
	return get_return_value_from_status(env, status);
}

static ERL_NIF_TERM
nif_scheduler_requestResources(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
	unsigned int length ;
	//ERL_NIF_TERM head, tail;

	state_ptr state = (state_ptr) enif_priv_data(env);
	
	if(state->initilised == 0 ) 
	{
		return enif_make_tuple2(env, 
			enif_make_atom(env, "state_error"), 
			enif_make_string(env, "Scheduler has not been initiated. Call scheduler_init first.", ERL_NIF_LATIN1));
	}

	if(!enif_is_list(env, argv[0])) 
		{
			return enif_make_tuple2(env, 
					enif_make_atom(env, "argument_error"), 
					enif_make_string(env, "Invalid or corrupted array of Request objects", ERL_NIF_LATIN1));
		};

	if(!enif_get_list_length(env, argv[0], &length))
	{
		return enif_make_tuple2(env, 
					enif_make_atom(env, "argument_error"), 
					enif_make_string(env, "Invalid or corrupted array of Request objects", ERL_NIF_LATIN1));

	}
	
	ErlNifBinary binary_arr[length];
	if(!inspect_array_of_binary_objects(env, argv[0], &binary_arr ))
	{
		return enif_make_tuple2(env, 
							enif_make_atom(env, "argument_error"), 
							enif_make_string(env, "Invalid or corrupted Request objects", ERL_NIF_LATIN1));
	}
	
	SchedulerDriverStatus status =  scheduler_requestResources( state->scheduler_state, &binary_arr);
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
			enif_make_atom(env, "state_error"), 
			enif_make_string(env, "Scheduler has not been initiated. Call scheduler_init first.", ERL_NIF_LATIN1));
	}

	if(!enif_is_list(env, argv[0])) 
	{
		return enif_make_tuple2(env, 
				enif_make_atom(env, "argument_error"), 
				enif_make_string(env, "Invalid or corrupted array of TaskStatus objects", ERL_NIF_LATIN1));
	};

	if(!enif_get_list_length(env, argv[0], &length))
	{
		return enif_make_tuple2(env, 
					enif_make_atom(env, "argument_error"), 
					enif_make_string(env, "Invalid or corrupted array of TaskStatus objects", ERL_NIF_LATIN1));

	}
	
	ErlNifBinary binary_arr[length];
	if(!inspect_array_of_binary_objects(env, argv[0], &binary_arr ))
	{
		return enif_make_tuple2(env, 
							enif_make_atom(env, "argument_error"), 
							enif_make_string(env, "Invalid or corrupted TaskStatus objects", ERL_NIF_LATIN1));
	}	

	SchedulerDriverStatus status =  scheduler_reconcileTasks( state->scheduler_state, &binary_arr);
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
			enif_make_atom(env, "state_error"), 
			enif_make_string(env, "Scheduler has not been initiated. Call scheduler_init first.", ERL_NIF_LATIN1));
	}

	if (!enif_inspect_binary(env, argv[0], &offerId_binary)) 
	{
		return enif_make_tuple2(env, 
					enif_make_atom(env, "argument_error"), 
					enif_make_string(env, "Invalid or corrupted OfferID", ERL_NIF_LATIN1));
	}
	
	if(!enif_is_list(env, argv[1])) 
	{
		return enif_make_tuple2(env, 
				enif_make_atom(env, "argument_error"), 
				enif_make_string(env, "Invalid or corrupted array of TaskInfo objects", ERL_NIF_LATIN1));
	};

	if(!enif_get_list_length(env, argv[1], &length))
	{
		return enif_make_tuple2(env, 
					enif_make_atom(env, "argument_error"), 
					enif_make_string(env, "Invalid or corrupted array of TaskInfo objects", ERL_NIF_LATIN1));

	}
	
	ErlNifBinary task_info_binary_arr[length];
	if(!inspect_array_of_binary_objects(env, argv[0], &task_info_binary_arr ))
	{
		return enif_make_tuple2(env, 
							enif_make_atom(env, "argument_error"), 
							enif_make_string(env, "Invalid or corrupted TaskInfo objects", ERL_NIF_LATIN1));
	}	

	if (!enif_inspect_binary(env, argv[2], &filters_binary)) 
	{
		return enif_make_tuple2(env, 
					enif_make_atom(env, "argument_error"), 
					enif_make_string(env, "Invalid or corrupted Filters", ERL_NIF_LATIN1));
	}
	SchedulerDriverStatus status =  scheduler_launchTasks(state->scheduler_state, &offerId_binary, &task_info_binary_arr, &filters_binary);
	return get_return_value_from_status(env, status);
}

static ErlNifFunc nif_funcs[] = {
	{"nif_scheduler_init", 3, nif_scheduler_init},
	{"nif_scheduler_init", 4, nif_scheduler_init},
	{"nif_scheduler_start", 0, nif_scheduler_start},
	{"nif_scheduler_join", 0, nif_scheduler_join},
	{"nif_scheduler_abort", 0, nif_scheduler_abort},
	{"nif_scheduler_stop", 1, nif_scheduler_stop},
	{"nif_scheduler_declineOffer", 2,nif_scheduler_declineOffer},
	{"nif_scheduler_killTask", 1,nif_scheduler_killTask},
	{"nif_scheduler_reviveOffers", 0 , nif_scheduler_reviveOffers},
	{"nif_scheduler_sendFrameworkMessage", 3, nif_scheduler_sendFrameworkMessage},
	{"nif_scheduler_requestResources",1, nif_scheduler_requestResources},
	{"nif_scheduler_reconcileTasks",1,nif_scheduler_reconcileTasks},
	{"nif_scheduler_launchTasks",3,nif_scheduler_launchTasks}
};

ERL_NIF_INIT(erlang_mesos, nif_funcs, load, NULL, NULL, unload);