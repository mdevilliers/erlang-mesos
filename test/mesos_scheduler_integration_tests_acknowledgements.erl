-module (mesos_scheduler_integration_tests_acknowledgements).

-include_lib("eunit/include/eunit.hrl").
-include ("mesos_pb.hrl").
-include ("mesos_erlang.hrl").

% these tests connect to a running instance of mesos
% change the location used here...
-define (MASTER_LOCATION, "127.0.0.1:5050").

explicit_acknowledgement_test() ->

    meck:new(test_framework, [non_strict]), 

    FrameworkInfo = #'FrameworkInfo'{user="", name="AcknowledgeTests"},

    % NOTE : returns false to enable explicit acknowledgements
    meck:expect(test_framework, init , fun(_) -> { FrameworkInfo, ?MASTER_LOCATION, false , []} end),
    
    meck:expect(test_framework, registered , fun(_FrameworkID, _MasterInfo, State) -> {ok,State} end),

    meck:expect(test_framework, resourceOffers , fun(Offer, State) -> 

		Scalar = mesos_pb:enum_symbol_by_value('Value.Type', 0),
    	Resource1 = #'Resource'{name="cpus", type=Scalar, scalar=#'Value.Scalar'{value=1}},

    	Command = "sleep 10",

    	{_,{H,M,S}} = calendar:local_time(),
    	Id = integer_to_list(H) ++ integer_to_list(M) ++ integer_to_list(S),

	    TaskInfo = #'TaskInfo'{
	        name = "erlang_task" ++ Id,
	        task_id = #'TaskID'{ value = "task_id_" ++ Id},
	        slave_id = Offer#'Offer'.slave_id,
	        resources = [Resource1],
	        command = #'CommandInfo'{value = Command}
	    },
    
    	{ok,driver_running} = scheduler:launchTasks(Offer#'Offer'.id, [TaskInfo]),

    	{ok,State} 
    end),

    meck:expect(test_framework, statusUpdate, fun(TaskStatus, State) ->
    	scheduler:acknowledgeStatusUpdate(TaskStatus),
    	{ok,State}
    end),

    {ok, _} = scheduler:start_link( test_framework, ?MASTER_LOCATION),

    timer:sleep(1000),

    scheduler:stop(0),
    scheduler:destroy(),

    meck:unload(test_framework).