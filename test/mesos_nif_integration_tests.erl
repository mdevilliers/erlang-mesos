-module (mesos_nif_integration_tests).
-include_lib("eunit/include/eunit.hrl").
-include ("mesos_pb.hrl").
-include ("mesos_erlang.hrl").

% these tests connect to a running instance of mesos
% change the location used here...
-define (MASTER_LOCATION, "127.0.1.1:5050").


init_and_clean_exit_init_scheduler_test()->

    example_framework:init(),
    timer:sleep(1000),
    example_framework:exit(),
    timer:sleep(1000),
    example_framework:init(),
    timer:sleep(1000),
    example_framework:exit().

inited_scheduler_can_not_be_reinited_test()->
    
    meck:new(test_framework, [non_strict]), 

    meck:expect(test_framework, registered , fun(State, _FrameworkID, _MasterInfo) -> {ok,State} end),
    meck:expect(test_framework, resourceOffers , fun(State, _Offer) -> {ok,State} end),

    FrameworkInfo = #'FrameworkInfo'{user="", name="Erlang Test Framework"},
    MasterLocation = ?MASTER_LOCATION,

    ok = scheduler:init(test_framework, FrameworkInfo, MasterLocation, []),

    timer:sleep(1000),
    {state_error, scheduler_already_inited} = scheduler:init(test_framework, FrameworkInfo, MasterLocation, []),

    meck:unload(test_framework).



