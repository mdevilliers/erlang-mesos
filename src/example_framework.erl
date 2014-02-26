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

resourceOffers(#framework_state{task_id=TaskNumber, task_limit=TaskNumber} = State, Offer) ->
    io:format("Reached max tasks ~p so declining offer.~n", [TaskNumber]),
    gen_scheduler:declineOffer(Offer#'Offer'.id),
    {ok,State};
resourceOffers(State, Offer) ->
    io:format("ResourceOffers callback : ~p ~n", [Offer]),

    CurrentTaskId = State#framework_state.task_id, 

    CurrentTaskId1 = CurrentTaskId + 1,
    State1 = State#framework_state{task_id = CurrentTaskId1},

    io:format("Launching Task : ~p", [CurrentTaskId]),

    Scalar = mesos_pb:enum_symbol_by_value('Value.Type', 0),
    Resource1 = #'Resource'{name="cpus", type=Scalar, scalar=#'Value.Scalar'{value=1}},
    Resource2 = #'Resource'{name="mem", type=Scalar, scalar=#'Value.Scalar'{value=128}},

    CommandInfoUri = #'CommandInfo.URI'{ value = filename:absname("../scripts/example_executor.es") },

    Executor = #'ExecutorInfo'{
      executor_id = #'ExecutorID'{value = integer_to_list(CurrentTaskId1)},
      command = #'CommandInfo'{value = filename:absname("../scripts/example_executor.es"), uris = [CommandInfoUri] },
      name = "sleep",
      source = "Erlang Test Framework"
    },

    TaskInfo = #'TaskInfo'{
        name = "ErlangTask",
        task_id = #'TaskID'{ value = integer_to_list(CurrentTaskId1)},
        slave_id = Offer#'Offer'.slave_id,
        resources = [Resource1,Resource2],
        executor = Executor
    },

    {ok,driver_running} = gen_scheduler:launchTasks(Offer#'Offer'.id, [TaskInfo]),
    {ok,State1}.

disconnected(State) ->
    io:format("Disconnected callback"),
    {ok,State}.

offerRescinded(State, OfferID) ->
    io:format("OfferRescinded callback : ~p ~n", [OfferID]),
    {ok,State}.
 
statusUpdate(#framework_state{task_id=TaskNumber} = State, {'TaskStatus',{'TaskID',_},'TASK_LOST',_,_,_}) ->
    io:format("StatusUpdate callback : ~p  -> decrementing current tasks.~n", ['TASK_LOST']),
    TaskNumber1 = TaskNumber - 1,
    State1 = State#framework_state{task_id = TaskNumber1},
    {ok,State1};
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
