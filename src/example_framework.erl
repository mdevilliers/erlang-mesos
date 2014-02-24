-module (example_framework).
-behaviour (gen_scheduler).

-include_lib("include/mesos.hrl").

% api
-export ([init/0]).

% from gen_scheduler
-export ([registered/3, 
          reregistered/2, 
          disconnected/1, 
          offerRescinded/2, 
          statusUpdate/2, 
          frameworkMessage/4, 
          slaveLost/2, 
          executorLost/4, 
          error/2,
          resourceOffers/2]).

-record (framework_state, {task_id = 0,task_limit = 5 }).

% api
init()->
    FrameworkInfo = #'FrameworkInfo'{user="John Smith", name="Erlang Test Framework"},
    MasterLocation = "127.0.1.1:5050",
    State = #framework_state{},
    io:format("State : ~p~n", [State]),
    ok = gen_scheduler:init(?MODULE, FrameworkInfo, MasterLocation, State),
    {ok,Status} = gen_scheduler:start(),
    Status.

% call backs
registered(State, FrameworkID, MasterInfo) ->
    io:format("Registered callback : ~p ~p~n", [FrameworkID, MasterInfo]),
    {ok,State}.

reregistered(State, MasterInfo) ->
    io:format("ReRegistered callback : ~p ~n", [MasterInfo]),
    {ok,State}.

resourceOffers(State, Offer) ->
    io:format("ResourceOffers callback : ~p ~n", [Offer]),

    CurrentTaskId = State#framework_state.task_id, 
    State1 = State#framework_state{task_id = CurrentTaskId + 1},
    io:format("Launching Task : ~p", [CurrentTaskId]),

    Scalar = mesos:enum_symbol_by_value('Value.Type', 0),
    Resource1 = #'Resource'{name="cpus", type=Scalar, scalar=#'Value.Scalar'{value=1}},
    Resource2 = #'Resource'{name="mem", type=Scalar, scalar=#'Value.Scalar'{value=128}},

    Executor = abc,

    TaskInfo = #'TaskInfo'{
              name = "ErlangTask",                          % = 1, string
              task_id = #'TaskID'{value = CurrentTaskId},                      % = 2, {msg,'TaskID'}
              slave_id = Offer#'Offer'.slave_id,                     % = 3, {msg,'SlaveID'}
              resources = [Resource1,Resource2],                % = 4, [{msg,'Resource'}]
              executor = Executor
    },
    gen_scheduler:launchTasks(Offer#'Offer'.id, TaskInfo),
    {ok,State1}.

disconnected(State) ->
    io:format("Disconnected callback"),
    {ok,State}.

offerRescinded(State, OfferID) ->
    io:format("OfferRescinded callback : ~p ~n", [OfferID]),
    {ok,State}.

statusUpdate(State, StatusUpdate) ->
    io:format("StatusUpdate callback : ~p ~n", [StatusUpdate]),
    {ok,State}. 

frameworkMessage(State, ExecutorID, SlaveID, Message) ->
    io:format("FrameworkMessage callback : ~p ~p ~p ~n", [ExecutorID, SlaveID, Message]),
    {ok,State}.

slaveLost(State, SlaveID) ->
    io:format("SlaveLost callback : ~p ~n", [SlaveID]),
    {ok,State}.

executorLost(State, ExecutorID, SlaveID, Status) ->
    io:format("ExecutorLost callback : ~p ~p ~p ~n", [ExecutorID, SlaveID, Status]),
    {ok,State}.

error(State, Message) ->
    io:format("Error callback : ~p ~n", [Message]),
    {ok,State}.
