-module (mesos_scheduler_integration_tests).
-include_lib("eunit/include/eunit.hrl").
-include ("mesos_pb.hrl").
-include ("mesos_erlang.hrl").

% these tests connect to a running instance of mesos
% change the location used here...
-define (MASTER_LOCATION, "127.0.0.1:5050").

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

crash_in_scheduler_closes_nif_connections_gracefully_test()->
    
    % naughty framework
    meck:new(test_framework, [non_strict]), 

    FrameworkInfo = #'FrameworkInfo'{user="", name="Erlang Test Framework"},
    meck:expect(test_framework, init , fun(_) -> { FrameworkInfo, ?MASTER_LOCATION, []} end),
    meck:expect(test_framework, registered , fun(_FrameworkID, _MasterInfo,_State) -> meck:exception(error, boom) end),
    meck:expect(test_framework, resourceOffers , fun(_Offer, State) -> {ok,State} end),

    {ok,_} = scheduler:start(test_framework, ?MASTER_LOCATION),
 
    io:format(user, "waiting for crash~n", []),
    timer:sleep(10), % wait for crash
    io:format(user, "starting up again~n", []),
    meck:unload(test_framework),

    % nice framework
    meck:new(test_framework, [non_strict]), 
    meck:expect(test_framework, init , fun(_) -> { FrameworkInfo, ?MASTER_LOCATION, []} end),
    meck:expect(test_framework, registered , fun(_FrameworkID, _MasterInfo, State) -> {ok,State} end),
    meck:expect(test_framework, resourceOffers , fun(_Offer, State) -> {ok,State} end),

    {ok,_} = scheduler:start(test_framework, ?MASTER_LOCATION),

    {ok, driver_stopped} = scheduler:stop(0),
    ok = scheduler:destroy(),
    
    meck:unload(test_framework).
