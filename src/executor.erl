%% -------------------------------------------------------------------
%% Copyright (c) 2015 Mark deVilliers.  All Rights Reserved.
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
-behaviour(gen_server).

%api
-export ([  start/2,
            start_link/2,
            join/0,
            abort/0,
            stop/0,
            sendFrameworkMessage/1,
            sendStatusUpdate/1,
            destroy/0]).

%gen server
-export([init/1, handle_call/3, handle_info/2, terminate/2, handle_cast/2,code_change/3]).

-include_lib("mesos_pb.hrl").
-include_lib("mesos_erlang.hrl").

%% callback specifications
-callback init(Args :: any()) -> {ok, State :: any}.

-callback registered( ExecutorInfo :: #'ExecutorInfo'{}, 
                      FrameworkInfo :: #'FrameworkInfo'{}, 
                      SlaveInfo :: #'SlaveInfo'{},
                      State :: any())-> {ok, State :: any()}.

-callback reregistered(SlaveInfo :: #'SlaveInfo'{}, State :: any()) -> {ok, State :: any()}.

-callback disconnected(State :: any()) -> {ok, State :: any()}.

-callback launchTask(TaskInfo :: #'TaskInfo'{}, State :: any()) -> {ok, State :: any()}.

-callback killTask(TaskID :: #'TaskID'{}, State :: any()) -> {ok, State :: any()}.

-callback frameworkMessage(Message :: string(), State :: any()) -> {ok, State :: any()}.

-callback shutdown(State :: any()) -> {ok, State :: any()}.

-callback error(Message :: string(), State :: any()) -> {ok, State :: any()}.    

%% -----------------------------------------------------------------------------------------

%% implementation

%% -----------------------------------------------------------------------------------------

-record(state, {
    handler_module,   %% Handler callback module
    handler_state %% Handler state
}).

%% -----------------------------------------------------------------------------------------

-spec start( Module :: atom(), Args :: term()) ->
    {ok, Server :: pid()} | {error, Reason :: term()}.
start(Module, Args) ->
    gen_server:start(?MODULE, {Module, Args}, []).

%% -----------------------------------------------------------------------------------------

-spec start_link( Module :: atom(), Args :: term()) ->
    {ok, Server :: pid()} | {error, Reason :: term()}.
start_link(Module, Args ) ->
    gen_server:start_link(?MODULE, {Module, Args}, []).

%% -----------------------------------------------------------------------------------------

-spec join() -> {ok, driver_running } | { error, executor_not_inited} | {error, driver_state()}.
join() ->
    nif_executor:join().

%% -----------------------------------------------------------------------------------------

-spec abort() -> {ok, driver_running } | { error, executor_not_inited} | {error, driver_state()}.
abort() ->
    nif_executor:abort().

%% -----------------------------------------------------------------------------------------

-spec stop() -> {ok, driver_running } | { error, executor_not_inited} | {error, driver_state()}.
stop() ->       
    nif_executor:stop().

%% -----------------------------------------------------------------------------------------

-spec sendFrameworkMessage( Message :: string() ) -> 
                          {ok, driver_running } 
                        | {error, {invalid_or_corrupted_parameter, data }}
                        | {error, executor_not_inited} 
                        | {error, driver_state()}.

sendFrameworkMessage(Data) when is_list(Data) ->
    nif_executor:sendFrameworkMessage(Data).
%% -----------------------------------------------------------------------------------------

-spec sendStatusUpdate( TaskStatus :: #'TaskStatus'{} ) -> 
                          {ok, driver_running } 
                        | {error, {invalid_or_corrupted_parameter, task_status }}
                        | {error, executor_not_inited} 
                        | {error, driver_state()}.

sendStatusUpdate(TaskStatus) when is_record(TaskStatus, 'TaskStatus') ->
    nif_executor:sendStatusUpdate(TaskStatus).
%% -----------------------------------------------------------------------------------------

-spec destroy() -> ok | {error, executor_not_inited}.

destroy() ->
    Response = nif_executor:destroy(),

    case whereis(?MODULE) of
        undefined  -> ok;
        _ -> unregister(?MODULE)
    end,
    
    Response.
    
%% -----------------------------------------------------------------------------------------
%% -----------------------------------------------------------------------------------------
%% -----------------------------------------------------------------------------------------
%% -----------------------------------------------------------------------------------------
%% Gen Server Implementation
%% -----------------------------------------------------------------------------------------
%% -----------------------------------------------------------------------------------------
%% -----------------------------------------------------------------------------------------
%% -----------------------------------------------------------------------------------------
init({Module, Args}) ->
    
     case whereis(?MODULE) of
        undefined ->
            register(?MODULE, self()),
            case Module:init(Args) of
             {ok, State} ->
                    ok = nif_executor:init(self()),
                    {ok,driver_running} = nif_executor:start(),               
                    {ok, #state{
                                handler_module = Module,
                                handler_state = State
                            }};
             Else ->  
                Error = {bad_return_value, Else},   
                {stop, Error}                                           
            end;
        Pid ->
            {stop, {already_started,Pid}} 
    end.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.


handle_info({registered , ExecutorInfoBin, FrameworkInfoBin, SlaveInfoBin }, #state{ handler_module = Module, handler_state = HandlerState }) ->
    ExecutorInfo = mesos_pb:decode_msg(ExecutorInfoBin, 'ExecutorInfo'),
    FrameworkInfo = mesos_pb:decode_msg(FrameworkInfoBin, 'FrameworkInfo'),
    SlaveInfo = mesos_pb:decode_msg(SlaveInfoBin, 'SlaveInfo'),

    {ok, State1} = Module:registered(ExecutorInfo, FrameworkInfo, SlaveInfo, HandlerState),
    {noreply, #state{ handler_module = Module, handler_state = State1 }};

handle_info({reregistered, SlaveInfoBin}, #state{ handler_module = Module, handler_state = HandlerState }) ->
    SlaveInfo = mesos_pb:decode_msg(SlaveInfoBin, 'SlaveInfo'),

    {ok, State1} = Module:reregistered(SlaveInfo, HandlerState),
    {noreply, #state{ handler_module = Module, handler_state = State1 }};

handle_info({disconnected}, #state{ handler_module = Module, handler_state = HandlerState }) ->

    {ok, State1} = Module:disconnected(HandlerState),
    {noreply, #state{ handler_module = Module, handler_state = State1 }};

handle_info({launchTask, TaskInfoBin}, #state{ handler_module = Module, handler_state = HandlerState }) ->
    TaskInfo = mesos_pb:decode_msg(TaskInfoBin, 'TaskInfo'),

    {ok, State1} = Module:launchTask(TaskInfo, HandlerState),
    {noreply, #state{ handler_module = Module, handler_state = State1 }};

handle_info({killTask, TaskIDBin} , #state{ handler_module = Module, handler_state = HandlerState }) ->
    TaskID = mesos_pb:decode_msg(TaskIDBin, 'TaskID'),
    
    {ok, State1} = Module:killTask(TaskID, HandlerState),
    {noreply, #state{ handler_module = Module, handler_state = State1 }};

handle_info({frameworkMessage, Message}, #state{ handler_module = Module, handler_state = HandlerState }) ->
    {ok, State1} = Module:frameworkMessage(Message, HandlerState),
    {noreply, #state{ handler_module = Module, handler_state = State1 }};

handle_info({shutdown}, #state{ handler_module = Module, handler_state = HandlerState }) ->
    {ok, State1} = Module:shutdown(HandlerState),
    {noreply, #state{ handler_module = Module, handler_state = State1 }};

handle_info({error, Message}, #state{ handler_module = Module, handler_state = HandlerState }) ->
    {ok, State1} = Module:error(Message, HandlerState),
    {noreply, #state{ handler_module = Module, handler_state = State1 }}.

code_change(_, State, _) ->
  {ok, State}.

terminate(_Reason, _State) ->
    do_terminate(),
    ok.

% helpers
do_terminate()->
    executor:stop(),
    executor:destroy().