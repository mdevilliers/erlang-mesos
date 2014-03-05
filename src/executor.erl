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

-include_lib("include/mesos_pb.hrl").

-type driver_state() :: driver_not_started |  driver_running | driver_aborted | driver_stopped | unknown.

% callback specifications
-callback registered(State :: any(), 
            ExecutorInfo :: #'ExecutorInfo'{}, 
            FrameworkInfo :: #'FrameworkInfo'{}, 
            SlaveInfo :: #'SlaveInfo'{})-> {ok, State :: any()}.

-callback reregistered(State :: any(),  
            SlaveInfo :: #'SlaveInfo'{})-> {ok, State :: any()}.

-callback disconnected(State :: any())-> {ok, State :: any()}.

-callback launchTask(State :: any(),  
            TaskInfo :: #'TaskInfo'{})-> {ok, State :: any()}.

-callback killTask(State :: any(),  
            TaskID :: #'TaskID'{})-> {ok, State :: any()}.

-callback frameworkMessage(State :: any(),  
            Message :: string())-> {ok, State :: any()}.

-callback shutdown(State :: any())-> {ok, State :: any()}.

-callback error(State :: any(),  
            Message :: string())-> {ok, State :: any()}.    

% implementation
-spec init(Module :: module(), State :: any()) ->  { state_error, executor_already_inited}
                        | {argument_error, invalid_or_corrupted_parameter, pid }
                        | ok.
init(Module, State) ->
    Pid = spawn(?MODULE, loop, [Module, State]),
    register(executor_loop, Pid),
    nif_executor:init(Pid).

-spec start() -> { state_error, executor_not_inited} 
                    | {ok, driver_running } 
                    | {error, driver_state()}.
start() ->
    nif_executor:start().

-spec join() -> { state_error, executor_not_inited} 
                    | {ok, driver_running } 
                    | {error, driver_state()}.
join() ->
    nif_executor:join().

-spec abort() -> { state_error, executor_not_inited} 
                    | {ok, driver_aborted } 
                    | {error, driver_state()}.
abort() ->
    nif_executor:abort().

-spec stop() -> { state_error, executor_not_inited} 
                    | {ok, driver_stopped } 
                    | {error, driver_state()}.
stop() ->       
    nif_executor:stop().


-spec sendFrameworkMessage( Message :: string() ) -> 
                          {ok, driver_running } 
                        | {argument_error, invalid_or_corrupted_parameter, data }
                        | {state_error, executor_not_inited} 
                        | {error, driver_state()}.

sendFrameworkMessage(Data) when is_list(Data) ->
    nif_executor:sendFrameworkMessage(Data).

-spec sendStatusUpdate( TaskStatus :: #'TaskStatus'{} ) -> 
                          {ok, driver_running } 
                        | {argument_error, invalid_or_corrupted_parameter, task_status }
                        | {state_error, executor_not_inited} 
                        | {error, driver_state()}.

sendStatusUpdate(TaskStatus) when is_record(TaskStatus, 'TaskStatus') ->
    nif_executor:sendStatusUpdate(TaskStatus).

-spec destroy() -> ok | {state_error, executor_not_inited}.

destroy() ->
    case nif_executor:destroy() of
        ok->
            executor_loop ! {internal_shudown},
            ok;
        Other ->
            Other
    end.

% private
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
            io:format("EXECUTOR: UNKNOWN MESSAGE : ~p~n", [Any]),
            loop(Module,State)
    end.
