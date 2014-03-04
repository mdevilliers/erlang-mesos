-module (executor).

%api
-export ([  init/2,
            start/0,
            join/0,
            abort/0,
            stop/0,
            sendFrameworkMessage/1,
            sendStatusUpdate/1,
            destroy/0]).

% private
-export ([loop/2]).
-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [ {registered, 4}, 
      {reregistered, 2}, 
      {disconnected, 1}, 
      {launchTask, 2},  
      {killTask, 2}, 
      {frameworkMessage, 2}, 
      {shutdown, 1} , 
      {error, 2}];
behaviour_info(_Other) ->
    undefined.

-include_lib("include/mesos.hrl").

init(Module, State)->
    Pid = spawn(?MODULE, loop, [Module, State]),
    register(executor_loop, Pid),
    Result = nif_executor:init(Pid),
    Result.

start() ->
    nif_executor:start().
join() ->
    nif_executor:join().
abort() ->
    nif_executor:abort().
stop() ->       
    nif_executor:stop().

sendFrameworkMessage(Data) ->
    nif_executor:sendFrameworkMessage(Data).
sendStatusUpdate(TaskStatus)->
    nif_executor:sendStatusUpdate(TaskStatus).
destroy() ->
    case nif_executor:destroy() of
        {ok, Status} ->
            executor_loop ! {internal_shudown},
            {ok, Status};
        Other ->
            Other
    end.

loop(Module,State) -> 
    receive     
        {registered , ExecutorInfoBin, FrameworkInfoBin, SlaveInfoBin } ->            
                ExecutorInfo = mesos_pb:decode_msg(ExecutorInfoBin, 'ExecutorInfo'),
                FrameworkInfo = mesos_pb:decode_msg(FrameworkInfoBin, 'FrameworkInfo'),
                SlaveInfo = mesos_pb:decode_msg(SlaveInfoBin, 'SlaveInfo'),
                {ok, State1} = Module:registered(State, ExecutorInfo, FrameworkInfo, SlaveInfo),
                loop(Module, State1);
        {reregistered, SlaveInfoBin} ->
        		SlaveInfo = mesos_pb:decode_msg(SlaveInfoBin, 'SlaveInfo'),
                {ok, State1} = Module:reregistered(State,SlaveInfo),
                loop(Module,State1);
        {disconnected} ->
                {ok, State1} = Module:disconnected(State),
                loop(Module, State1);   
        {launchTask, TaskInfoBin} ->
                TaskInfo = mesos_pb:decode_msg(TaskInfoBin, 'TaskInfo'),
                {ok, State1} = Module:launchTask(State,TaskInfo),
                loop(Module,State1);
        {killTask, TaskIDBin} ->
                TaskID = mesos_pb:decode_msg(TaskIDBin, 'TaskID'),
                {ok, State1} = Module:killTask(State,TaskID),
                loop(Module,State1);
        {frameworkMessage, Message} ->
                {ok, State1} = Module:frameworkMessage(State,Message),
                loop(Module, State1);
        {shutdown} ->
                {ok, State1} = Module:shutdown(State),
                loop(Module,State1);
        {error, Message} ->
                {ok, State1} = Module:error(State,Message),
                loop(Module,State1);
        {internal_shudown} ->
                unregister(executor_loop),
                {shutdown_complete};          
        Any ->
            io:format("Other message from nif : ~p~n", [Any]),
            loop(Module,State)
    after
        1000 ->
            loop(Module,State)
    end.
