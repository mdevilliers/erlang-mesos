%% -------------------------------------------------------------------
%% Copyright (c) 2014 Mark deVilliers.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------


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

%% private
-export ([loop/2]).

-include_lib("mesos_pb.hrl").
-include_lib("mesos_erlang.hrl").

%% callback specifications
-callback registered(State :: any(), 
            ExecutorInfo :: #'ExecutorInfo'{}, 
            FrameworkInfo :: #'FrameworkInfo'{}, 
            SlaveInfo :: #'SlaveInfo'{})-> {ok, State :: any()}.

-callback reregistered(State :: any(), SlaveInfo :: #'SlaveInfo'{}) -> {ok, State :: any()}.

-callback disconnected(State :: any()) -> {ok, State :: any()}.

-callback launchTask(State :: any(), TaskInfo :: #'TaskInfo'{}) -> {ok, State :: any()}.

-callback killTask(State :: any(), TaskID :: #'TaskID'{}) -> {ok, State :: any()}.

-callback frameworkMessage(State :: any(), Message :: string()) -> {ok, State :: any()}.

-callback shutdown(State :: any()) -> {ok, State :: any()}.

-callback error(State :: any(), Message :: string()) -> {ok, State :: any()}.    

%% -----------------------------------------------------------------------------------------

%% implementation

-spec init(Module :: module(), State :: any()) ->  { state_error, executor_already_inited}
                        | {argument_error, invalid_or_corrupted_parameter, pid }
                        | ok.
init(Module, State) ->
    Pid = spawn(?MODULE, loop, [Module, State]),

    try register(executor_loop, Pid) of
        true -> nif_executor:init(Pid)
    catch
         error:badarg ->  {state_error, executor_already_inited}
    end.

%% -----------------------------------------------------------------------------------------

-spec start() -> { state_error, executor_not_inited} | {ok, driver_running } | {error, driver_state()}.
start() ->
    nif_executor:start().

%% -----------------------------------------------------------------------------------------

-spec join() -> { state_error, executor_not_inited} | {ok, driver_running } | {error, driver_state()}.
join() ->
    nif_executor:join().

%% -----------------------------------------------------------------------------------------

-spec abort() -> { state_error, executor_not_inited} | {ok, driver_aborted } | {error, driver_state()}.
abort() ->
    nif_executor:abort().

%% -----------------------------------------------------------------------------------------

-spec stop() -> { state_error, executor_not_inited} | {ok, driver_stopped } | {error, driver_state()}.
stop() ->       
    nif_executor:stop().

%% -----------------------------------------------------------------------------------------

-spec sendFrameworkMessage( Message :: string() ) -> 
                          {ok, driver_running } 
                        | {argument_error, invalid_or_corrupted_parameter, data }
                        | {state_error, executor_not_inited} 
                        | {error, driver_state()}.

sendFrameworkMessage(Data) when is_list(Data) ->
    nif_executor:sendFrameworkMessage(Data).
%% -----------------------------------------------------------------------------------------

-spec sendStatusUpdate( TaskStatus :: #'TaskStatus'{} ) -> 
                          {ok, driver_running } 
                        | {argument_error, invalid_or_corrupted_parameter, task_status }
                        | {state_error, executor_not_inited} 
                        | {error, driver_state()}.

sendStatusUpdate(TaskStatus) when is_record(TaskStatus, 'TaskStatus') ->
    nif_executor:sendStatusUpdate(TaskStatus).
%% -----------------------------------------------------------------------------------------

-spec destroy() -> ok | {state_error, executor_not_inited}.

destroy() ->
    case nif_executor:destroy() of
        ok->
            executor_loop ! {internal_shudown},
            ok;
        Other ->
            Other
    end.
    
%% -----------------------------------------------------------------------------------------

% private
loop(Module,State) -> 
    receive     
        {registered , ExecutorInfoBin, FrameworkInfoBin, SlaveInfoBin } ->            
                
                ExecutorInfo = mesos_pb:decode_msg(ExecutorInfoBin, 'ExecutorInfo'),
                FrameworkInfo = mesos_pb:decode_msg(FrameworkInfoBin, 'FrameworkInfo'),
                SlaveInfo = mesos_pb:decode_msg(SlaveInfoBin, 'SlaveInfo'),
                
                try Module:registered(State, ExecutorInfo, FrameworkInfo, SlaveInfo) of
                    {ok, State1} -> loop(Module,State1)
                catch
                   Class:Reason -> 
                        internal_shudown(Module),
                        exit(erlang:Class([
                                    {reason, Reason},
                                    {mfa, {Module, registered, 4}},
                                    {stacktrace, erlang:get_stacktrace()},
                                    {state, State}
                                ]))
                end;
        {reregistered, SlaveInfoBin} ->

        		SlaveInfo = mesos_pb:decode_msg(SlaveInfoBin, 'SlaveInfo'),
                try Module:reregistered(State,SlaveInfo) of
                    {ok, State1} -> loop(Module,State1)
                catch
                   Class:Reason -> 
                        internal_shudown(Module),
                        exit(erlang:Class([
                                    {reason, Reason},
                                    {mfa, {Module, reregistered, 2}},
                                    {stacktrace, erlang:get_stacktrace()},
                                    {state, State}
                                ]))
                end; 
        {disconnected} ->
                
                try Module:disconnected(State) of
                    {ok, State1} -> loop(Module,State1)
                catch
                   Class:Reason -> 
                        internal_shudown(Module),
                        exit(erlang:Class([
                                    {reason, Reason},
                                    {mfa, {Module, disconnected, 1}},
                                    {stacktrace, erlang:get_stacktrace()},
                                    {state, State}
                                ]))
                end; 
        {launchTask, TaskInfoBin} ->
                TaskInfo = mesos_pb:decode_msg(TaskInfoBin, 'TaskInfo'),
                
                try Module:launchTask(State,TaskInfo) of
                    {ok, State1} -> loop(Module,State1)
                catch
                   Class:Reason -> 
                        internal_shudown(Module),
                        exit(erlang:Class([
                                    {reason, Reason},
                                    {mfa, {Module, launchTask, 2}},
                                    {stacktrace, erlang:get_stacktrace()},
                                    {state, State}
                                ]))
                end;
        {killTask, TaskIDBin} ->
                TaskID = mesos_pb:decode_msg(TaskIDBin, 'TaskID'),
                
                try Module:killTask(State,TaskID) of
                    {ok, State1} -> loop(Module,State1)
                catch
                   Class:Reason -> 
                        internal_shudown(Module),
                        exit(erlang:Class([
                                    {reason, Reason},
                                    {mfa, {Module, killTask, 2}},
                                    {stacktrace, erlang:get_stacktrace()},
                                    {state, State}
                                ]))
                end;
        {frameworkMessage, Message} ->
                
                try Module:frameworkMessage(State,Message) of
                    {ok, State1} -> loop(Module,State1)
                catch
                   Class:Reason -> 
                        internal_shudown(Module),
                        exit(erlang:Class([
                                    {reason, Reason},
                                    {mfa, {Module, frameworkMessage, 2}},
                                    {stacktrace, erlang:get_stacktrace()},
                                    {state, State}
                                ]))
                end;
        {shutdown} ->

                try Module:shutdown(State) of
                    {ok, State1} -> loop(Module,State1)
                catch
                   Class:Reason -> 
                        internal_shudown(Module),
                        exit(erlang:Class([
                                    {reason, Reason},
                                    {mfa, {Module, shutdown, 1}},
                                    {stacktrace, erlang:get_stacktrace()},
                                    {state, State}
                                ]))
                end;
        {error, Message} ->

                try Module:error(State,Message) of
                    {ok, State1} -> loop(Module,State1)
                catch
                   Class:Reason -> 
                        internal_shudown(Module),
                        exit(erlang:Class([
                                    {reason, Reason},
                                    {mfa, {Module, error, 2}},
                                    {stacktrace, erlang:get_stacktrace()},
                                    {state, State}
                                ]))
                end;
        {internal_shudown} ->
                unregister(executor_loop),
                {shutdown_complete};          
        Any ->
            io:format("EXECUTOR: UNKNOWN MESSAGE : ~p~n", [Any]),
            loop(Module,State)
    end.

% helpers
internal_shudown(Module)->
    try 
        executor:stop(),
        executor:destroy()
    catch Class:Reason ->
        erlang:Class([
            {reason, Reason},
            {mfa, {Module, internal_shudown, 1}},
            {stacktrace, erlang:get_stacktrace()},
            {terminate_reason, Reason}
        ])
end.