-module (http_scheduler_integration_tests).

-include_lib("eunit/include/eunit.hrl").

http_scheduler_can_be_started_and_stopped_test() ->
	
	{ok,Pid} = scheduler:start(example_scheduler, []),
	ok = scheduler:teardown(Pid).


http_scheduler_can_be_restarted_test() ->
	
	{ok,Pid} = scheduler:start(example_scheduler, []),
	ok = scheduler:teardown(Pid),
	{ok,Pid2} = scheduler:start(example_scheduler, []),
	ok = scheduler:teardown(Pid2).