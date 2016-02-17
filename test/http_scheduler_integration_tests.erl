-module (http_scheduler_integration_tests).

-include_lib("eunit/include/eunit.hrl").
-include ("mesos_pb.hrl").
-include ("scheduler_pb.hrl").

-define (MASTER_LOCATION, "127.0.0.1:5050").

http_scheduler_can_be_started_and_stopped_test() ->
    
    meck:new(test_framework, [non_strict]), 

    meck:expect(test_framework, init , fun(_) -> 

        CurrentUser = os:getenv("USER"),
        FrameworkInfo = #'mesos.v1.FrameworkInfo'{ user=CurrentUser, 
                                               name="Erlang Test Framework"},

        { FrameworkInfo, ?MASTER_LOCATION, true, true, [] }
        end),
    
    meck:expect(test_framework, subscribed , fun(_Client, State) ->  {ok,State} end),
    meck:expect(test_framework, offers , fun(_Client, _Offers, State) ->  {ok,State} end),

    {ok,Pid} = scheduler:start(test_framework, []),
    
    meck:wait(test_framework, subscribed, '_', 1000),
    meck:wait(test_framework, offers, '_', 1000),

    ok = scheduler:teardown(Pid),

    meck:unload(test_framework).


 http_scheduler_can_be_restarted_test() ->

    http_scheduler_can_be_started_and_stopped_test(),
    http_scheduler_can_be_started_and_stopped_test().
