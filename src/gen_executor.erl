-module (gen_executor).

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

-include_lib("include/mesos.hrl").

init(Module, State)->
    Pid = spawn(?MODULE, loop, [Module, State]),
    Result = executor:init(Pid),
    Result.

start() ->
    executor:start().
join() ->
    executor:join().
abort() ->
    executor:abort().
stop() ->
    executor:stop().
sendFrameworkMessage(Data) ->
    executor:sendFrameworkMessage(Data).
sendStatusUpdate(TaskStatus)->
    executor:sendStatusUpdate(TaskStatus).
destroy() ->
    executor:destroy().

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
        Any ->
            io:format("other message from nif : ~p~n", [Any]),
            loop(Module,State)
    after
        1000 ->
            loop(Module,State)
    end.
