#include "erl_nif.h"
#include "mesos_c_api.hpp"


static ERL_NIF_TERM get_atom_from_status(ErlNifEnv* env, SchedulerDriverStatus status)
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
static ERL_NIF_TERM get_return_value_from_status(ErlNifEnv* env, SchedulerDriverStatus status)
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