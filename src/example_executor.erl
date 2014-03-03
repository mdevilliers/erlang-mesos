-module (example_executor).

-behaviour (executor).

-include_lib("include/mesos.hrl").

% from gen_executor
-export ([registered/4, 
          reregistered/2, 
          disconnected/1, 
          launchTask/2,  
          killTask/2, 
          frameworkMessage/2, 
          shutdown/1, 
          error/2]).

% api
-export ([init/0]).    

init()->
    ok = executor:init(?MODULE, []),
    {ok,Status} = executor:start(),
    executor:sendFrameworkMessage("hello from the executor"),
    Status.

% call backs
registered(State, ExecutorInfo, FrameworkInfo, SlaveInfo) ->
    io:format("Registered callback : ~p ~p ~p~n", [ExecutorInfo, FrameworkInfo, SlaveInfo]),
    {ok,State}.

reregistered(State,SlaveInfo) ->
    io:format("Reregistered callback : ~p ~n", [SlaveInfo]),
    {ok,State}.

disconnected(State) ->
    io:format("Disconnected callback~n", []),
    {ok,State}.

launchTask(State,TaskInfo) ->
    io:format("LaunchTask callback : ~p ~n", [TaskInfo]),
    executor:sendStatusUpdate(#'TaskStatus'{task_id = TaskInfo#'TaskInfo'.task_id , state= mesos_pb:enum_symbol_by_value_TaskState(1)}),
    {ok,State}.

killTask(State,TaskID) ->
    io:format("KillTask callback : ~p ~n", [TaskID]),
    {ok,State}.

frameworkMessage(State,Message) ->
    io:format("FrameworkMessage callback : ~p ~n", [Message]),
    {ok,State}.

shutdown(State) ->
    io:format("Shutdown callback~n", []),
    {ok,State}.

error(State,Message) ->
    io:format("Error callback : ~p ~n", [Message]),
    {ok,State}.


