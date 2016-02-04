-module (mesos_scheduler_integration_tests).
-include_lib("eunit/include/eunit.hrl").
-include ("mesos_pb.hrl").
-include ("mesos_erlang.hrl").

% these tests connect to a running instance of mesos
% change the location used here...
-define (MASTER_LOCATION, "zk://localhost:2181/mesos").

init_and_clean_exit_init_scheduler_test()->

    {ok, _Pid} = scheduler:start_link( example_framework, ?MASTER_LOCATION),
    example_framework:exit(),
    {ok, _Pid2} = scheduler:start_link( example_framework, ?MASTER_LOCATION),
    example_framework:exit().

inited_scheduler_can_not_be_reinited_test()->
    
    meck:new(test_framework, [non_strict]), 

    FrameworkInfo = #'FrameworkInfo'{user="", name="Erlang Test Framework"},

    meck:expect(test_framework, init , fun(_) -> { FrameworkInfo, ?MASTER_LOCATION, []} end),
    meck:expect(test_framework, registered , fun(_FrameworkID, _MasterInfo, State) -> {ok,State} end),
    meck:expect(test_framework, resourceOffers , fun(_Offer, State) -> {ok,State} end),

    {ok, _} = scheduler:start_link( test_framework, ?MASTER_LOCATION),
    {error,{already_started, _}} = scheduler:start( test_framework, ?MASTER_LOCATION),

    {ok, driver_stopped} = scheduler:stop(0),
    ok = scheduler:destroy(),

    meck:unload(test_framework).

crash_in_scheduler_closes_nif_connections_gracefully_test()->
    
    % naughty framework
    meck:new(test_framework_two, [non_strict]), 

    FrameworkInfo = #'FrameworkInfo'{user="", name="Erlang Test Framework 2"},
    meck:expect(test_framework_two, init , fun(_) -> {FrameworkInfo, ?MASTER_LOCATION, []} end),
    meck:expect(test_framework_two, registered , fun(_FrameworkID, _MasterInfo,_State) -> 
        io:format(user, "registered called",[]),
        meck:exception(error, boom) end),
    meck:expect(test_framework_two, resourceOffers , fun(_Offer, State) -> {ok,State} end),

    {ok,_} = scheduler:start(test_framework_two, ?MASTER_LOCATION),
 
    io:format(user, "waiting for crash~n", []),
    timer:sleep(500), % wait for crash
    io:format(user, "starting up again~n", []),
    meck:unload(test_framework_two),

    % nice framework
    meck:new(test_framework_two, [non_strict]), 
    meck:expect(test_framework_two, init , fun(_) -> { FrameworkInfo, ?MASTER_LOCATION, []} end),
    meck:expect(test_framework_two, registered , fun(_FrameworkID, _MasterInfo, State) -> {ok,State} end),
    meck:expect(test_framework_two, resourceOffers , fun(_Offer, State) -> {ok,State} end),

    {ok,_} = scheduler:start(test_framework_two, ?MASTER_LOCATION),

    {ok, driver_stopped} = scheduler:stop(0),
    ok = scheduler:destroy(),
    
    meck:unload(test_framework_two).


unknown_message_to_scheduler_will_not_crash_scheduler_test() ->

    meck:new(test_framework, [non_strict]), 

    FrameworkInfo = #'FrameworkInfo'{user="", name="Erlang Test Framework"},

    meck:expect(test_framework, init , fun(_) -> { FrameworkInfo, ?MASTER_LOCATION, []} end),
    meck:expect(test_framework, registered , fun(_FrameworkID, _MasterInfo, State) -> {ok,State} end),
    meck:expect(test_framework, resourceOffers , fun(_Offer, State) -> {ok,State} end),

    {ok,CheekyPid} = scheduler:start( test_framework, ?MASTER_LOCATION),

    CheekyPid = whereis(scheduler),
    CheekyPid ! {booya},
    CheekyPid = whereis(scheduler),

    {ok, driver_stopped} = scheduler:stop(0),
    ok = scheduler:destroy(),
    
    meck:unload(test_framework).
