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


-module (scheduler).
-behaviour(gen_server).

%api
-export([
        start/2,
        start_link/2,
        join/0,
        abort/0,
        stop/1,
        declineOffer/1,
        declineOffer/2,
        killTask/1,
        reviveOffers/0,
        sendFrameworkMessage/3,
        requestResources/1,
        reconcileTasks/1,
        launchTasks/2,
        launchTasks/3,
        destroy/0]).

%gen server
-export([init/1, handle_call/3, handle_info/2, terminate/2, handle_cast/2,
    code_change/3]).

-include_lib("mesos_pb.hrl").
-include_lib("mesos_erlang.hrl").

% callback specifications
-callback init(State :: any()) -> {FrameworkInfo :: #'FrameworkInfo'{}, MasterLocation  :: string(), State :: any()} | 
                                    {FrameworkInfo :: #'FrameworkInfo'{}, MasterLocation  :: string(), Credential :: #'Credential'{}, State :: any()}.

-callback registered(State :: any(), 
            FrameworkInfo :: #'FrameworkInfo'{}, 
            MasterInfo :: #'MasterInfo'{}) -> {ok, State :: any()}.

-callback reregistered(State :: any(), MasterInfo :: #'MasterInfo'{}) -> {ok, State :: any()}.

-callback disconnected(State :: any()) -> {ok, State :: any()}.

-callback resourceOffers(State :: any(), Offer :: #'Offer'{}) -> {ok, State :: any()}.

-callback offerRescinded(State :: any(), OfferID :: #'OfferID'{}) -> {ok, State :: any()}.

-callback statusUpdate(State :: any(), TaskStatus :: #'TaskStatus'{}) -> {ok, State :: any()}.

-callback frameworkMessage(State :: any(),  
            ExecutorId :: #'ExecutorID'{},
            SlaveId :: #'SlaveID'{},
            Message :: string()) -> {ok, State :: any()}.

-callback slaveLost(State :: any(), SlaveId :: #'SlaveID'{}) -> {ok, State :: any()}.

-callback executorLost(State :: any(),  
            ExecutorId :: #'ExecutorID'{},
            SlaveId :: #'SlaveID'{},
            Status :: pos_integer()) -> {ok, State :: any()}.

-callback error(State :: any(), Message :: string()) -> {ok, State :: any()}.   

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

-spec join() -> {ok, driver_running } | { error, scheduler_not_inited} | {error, driver_state()}.
join() ->
    nif_scheduler:join().

%% -----------------------------------------------------------------------------------------

-spec abort() -> {ok, driver_aborted } | { state_error, scheduler_not_inited} | {error, driver_state()}.
abort() ->
    nif_scheduler:abort().

%% -----------------------------------------------------------------------------------------

-spec stop(integer()) -> {ok, driver_aborted } | { error, scheduler_not_inited} | {error, driver_state()}.    
stop(Failover) when is_integer(Failover), 
                                Failover > -1, 
                                Failover < 2 ->
     nif_scheduler:stop(Failover).

%% -----------------------------------------------------------------------------------------

-spec declineOffer( OfferId :: #'OfferID'{}) -> 
                      {ok, driver_running } 
                    | {error, scheduler_not_inited} 
                    | {error, {invalid_or_corrupted_parameter, offer_id}}
                    | {error, driver_state()}.

declineOffer(OfferId) when is_record(OfferId, 'OfferID') ->
    nif_scheduler:declineOffer(OfferId).

-spec declineOffer( OfferId :: #'OfferID'{},
                    Filter :: #'Filters'{}) ->
                      {ok, driver_running }
                    | {error, scheduler_not_inited} 
                    | {error, {invalid_or_corrupted_parameter, offer_id}}
                    | {error, {invalid_or_corrupted_parameter, filters}}
                    | {error, driver_state()}.

declineOffer(OfferId,Filter) when is_record(OfferId, 'OfferID'),
                                  is_record(Filter, 'Filters') ->
    nif_scheduler:declineOffer(OfferId, Filter).                               

%% -----------------------------------------------------------------------------------------

-spec killTask( TaskId :: #'TaskID'{}) -> 
                      {ok, driver_running } 
                    | {error, scheduler_not_inited} 
                    | {error, {invalid_or_corrupted_parameter, task_id}}
                    | {error, driver_state()}.

killTask(TaskId) when is_record(TaskId,'TaskID') ->
    nif_scheduler:killTask(TaskId).

%% -----------------------------------------------------------------------------------------

-spec reviveOffers() -> {ok, driver_aborted } | { error, scheduler_not_inited} | {error, driver_state()}.

reviveOffers()->
    nif_scheduler:reviveOffers().

%% -----------------------------------------------------------------------------------------

-spec sendFrameworkMessage( ExecutorId :: #'ExecutorID'{},
                            SlaveId :: #'SlaveID'{},
                            Data ::string()) -> 
                              {ok, driver_running }
                            | {error, scheduler_not_inited} 
                            | {error, {invalid_or_corrupted_parameter, executor_id}}
                            | {error, {invalid_or_corrupted_parameter, slave_id}}
                            | {error, {invalid_or_corrupted_parameter, data}}
                            | {error, driver_state()}.

sendFrameworkMessage(ExecutorId,SlaveId,Data) when is_record(ExecutorId, 'ExecutorID'),
                                                   is_record(SlaveId, 'SlaveID'),
                                                   is_list(Data)->
    nif_scheduler:sendFrameworkMessage(ExecutorId,SlaveId,Data).

%% -----------------------------------------------------------------------------------------

-spec requestResources( Requests :: [ #'Request'{} ]) -> 
                      {ok, driver_running } 
                    | {error, scheduler_not_inited} 
                    | {error, {invalid_or_corrupted_parameter, request_array}}
                    | {error, driver_state()}.

requestResources(Requests) when is_list(Requests) ->
    nif_scheduler:requestResources(Requests).

%% -----------------------------------------------------------------------------------------

-spec reconcileTasks( TaskStatus :: [ #'TaskStatus'{} ]) ->
                      {ok, driver_running }
                    | {error, scheduler_not_inited} 
                    | {error, {invalid_or_corrupted_parameter, task_status_array}}
                    | {error, driver_state()}.

reconcileTasks(TaskStatus)when is_list(TaskStatus)->
    nif_scheduler:reconcileTasks(TaskStatus).

%% -----------------------------------------------------------------------------------------

-spec launchTasks(  OfferId :: #'OfferID'{}, 
                    TaskInfos :: [ #'TaskInfo'{}]) ->
                      {ok, driver_running } 
                    | {error, scheduler_not_inited} 
                    | {error, {invalid_or_corrupted_parameter, offer_id}}
                    | {error, {invalid_or_corrupted_parameter, task_info_array}}
                    | {error, driver_state()}.

launchTasks(OfferId, TaskInfos) when is_record(OfferId, 'OfferID'), 
                                     is_list(TaskInfos) ->
    nif_scheduler:launchTasks(OfferId, TaskInfos).

-spec launchTasks(  OfferId :: #'OfferID'{}, 
                    TaskInfos :: [ #'TaskInfo'{}],
                    Filter :: #'Filters'{}) ->
                      {ok, driver_running }
                    | {error, scheduler_not_inited} 
                    | {error, {invalid_or_corrupted_parameter, offer_id}}
                    | {error, {invalid_or_corrupted_parameter, task_info_array}}
                    | {error, {invalid_or_corrupted_parameter, filters}}
                    | {error, driver_state()}.

launchTasks(OfferId, TaskInfos, Filter) when is_record(OfferId, 'OfferID'), 
                                             is_list(TaskInfos),
                                             is_record(Filter, 'Filters') ->
    nif_scheduler:launchTasks(OfferId, TaskInfos, Filter).

%% -----------------------------------------------------------------------------------------

-spec destroy() -> ok | {error, scheduler_not_inited}.
destroy() ->
    Response = nif_scheduler:destroy(),
    unregister(?MODULE),
    Response.

%% -----------------------------------------------------------------------------------------
%% -----------------------------------------------------------------------------------------
%% -----------------------------------------------------------------------------------------
%% Gen Server Implementation
%% -----------------------------------------------------------------------------------------
%% -----------------------------------------------------------------------------------------
%% -----------------------------------------------------------------------------------------
init({Module, Args}) ->
    
     case whereis(?MODULE) of
        undefined ->
            register(?MODULE, self()),
            case Module:init(Args) of
             {FrameworkInfo, MasterLocation, State} when is_record(FrameworkInfo, 'FrameworkInfo'), 
                                                         is_list(MasterLocation) ->
                    
                    ok = nif_scheduler:init(self(), FrameworkInfo, MasterLocation), 
                    {ok,driver_running} = nif_scheduler:start(),                                    
                    {ok, #state{
                                handler_module = Module,
                                handler_state = State
                            }};
             {FrameworkInfo, MasterLocation, Credential, State} when is_record(FrameworkInfo, 'FrameworkInfo'), 
                                                                is_record(Credential, 'Credential'),
                                                                is_list(MasterLocation) ->
                    ok = nif_scheduler:init(self(), FrameworkInfo, MasterLocation,Credential),
                    {ok,driver_running} = nif_scheduler:start(),  
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

handle_info({registered , FrameworkIdBin, MasterInfoBin }, #state{ handler_module = Module, handler_state = HandlerState }) ->
    
    FrameworkId = mesos_pb:decode_msg(FrameworkIdBin, 'FrameworkID'),
    MasterInfo = mesos_pb:decode_msg(MasterInfoBin, 'MasterInfo'),
    MasterInfo2 = MasterInfo#'MasterInfo'{ip = int_to_ip(MasterInfo#'MasterInfo'.ip)},

    {ok, State1} = Module:registered(HandlerState, FrameworkId, MasterInfo2),
    {noreply, #state{ handler_module = Module, handler_state = State1 }};

handle_info({resourceOffers, OfferBin}, #state{ handler_module = Module, handler_state = HandlerState }) ->

    Offer = mesos_pb:decode_msg(OfferBin, 'Offer'),
    {ok, State1} = Module:resourceOffers(HandlerState,Offer),
    {noreply, #state{ handler_module = Module, handler_state = State1 }};

handle_info({reregistered, MasterInfoBin}, #state{ handler_module = Module, handler_state = HandlerState }) ->

    MasterInfo = mesos_pb:decode_msg(MasterInfoBin, 'MasterInfo'),
    MasterInfo2 = MasterInfo#'MasterInfo'{ip = int_to_ip(MasterInfo#'MasterInfo'.ip)},
    {ok, State1} = Module:reregistered(HandlerState,MasterInfo2),
    {noreply, #state{ handler_module = Module, handler_state = State1 }};

handle_info({disconnected}, #state{ handler_module = Module, handler_state = HandlerState }) ->

    {ok, State1} = Module:disconnected(HandlerState),
    {noreply, #state{ handler_module = Module, handler_state = State1 }};

handle_info({offerRescinded, OfferIdBin}, #state{ handler_module = Module, handler_state = HandlerState }) ->
    OfferId = mesos_pb:decode_msg(OfferIdBin, 'OfferID'),
    {ok, State1} = Module:offerRescinded(HandlerState,OfferId),
    {noreply, #state{ handler_module = Module, handler_state = State1 }};

handle_info({statusUpdate, TaskStatusBin}, #state{ handler_module = Module, handler_state = HandlerState }) ->
    TaskStatus = mesos_pb:decode_msg(TaskStatusBin, 'TaskStatus'),
    {ok, State1} = Module:statusUpdate(HandlerState,TaskStatus),
    {noreply, #state{ handler_module = Module, handler_state = State1 }};

handle_info({frameworkMessage, ExecutorIdBin, SlaveIdBin, Message}, #state{ handler_module = Module, handler_state = HandlerState }) ->
    ExecutorId = mesos_pb:decode_msg(ExecutorIdBin, 'ExecutorID'),
    SlaveId = mesos_pb:decode_msg(SlaveIdBin, 'SlaveID'),
    {ok, State1} = Module:frameworkMessage(HandlerState,ExecutorId,SlaveId,Message),
    {noreply, #state{ handler_module = Module, handler_state = State1 }};

handle_info({slaveLost, SlaveIdBin}, #state{ handler_module = Module, handler_state = HandlerState }) ->
    SlaveId = mesos_pb:decode_msg(SlaveIdBin, 'SlaveID'),
    {ok, State1} = Module:slaveLost(HandlerState,SlaveId),
    {noreply, #state{ handler_module = Module, handler_state = State1 }};

handle_info({executorLost, ExecutorIdBin, SlaveIdBin, Status}, #state{ handler_module = Module, handler_state = HandlerState }) ->
    ExecutorId = mesos_pb:decode_msg(ExecutorIdBin, 'ExecutorID'),
    SlaveId = mesos_pb:decode_msg(SlaveIdBin, 'SlaveID'),
    {ok, State1} = Module:executorLost(HandlerState,ExecutorId,SlaveId,Status),
    {noreply, #state{ handler_module = Module, handler_state = State1 }};

handle_info({error, Message}, #state{ handler_module = Module, handler_state = HandlerState }) ->
    {ok, State1} = Module:error(HandlerState,Message),
    {noreply, #state{ handler_module = Module, handler_state = State1 }};

handle_info(_Info, State) ->
    % TODO : Resolve this
    io:format(user, "SCHEDULER: UNKNOWN MESSAGE : ~p~n", [_Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    do_terminate(),
    ok.

code_change(_, State, _) ->
  {ok, State}.

% helpers
int_to_ip(Ip)-> {Ip bsr 24, (Ip band 16711680) bsr 16, (Ip band 65280) bsr 8, Ip band 255}.

do_terminate() -> 
    unregister(?MODULE),
    scheduler:stop(0),
    scheduler:destroy().
