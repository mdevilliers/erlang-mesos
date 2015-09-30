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


-module (scheduler).
-behaviour(gen_server).

%api
-export([
        start/2,
        start_link/2,
        join/0,
        abort/0,
        stop/1,
        acceptOffers/2,
        acceptOffers/3,
        declineOffer/1,
        declineOffer/2,
        killTask/1,
        reviveOffers/0,
        sendFrameworkMessage/3,
        requestResources/1,
        reconcileTasks/1,
        launchTasks/2,
        launchTasks/3,
        destroy/0,
        acknowledgeStatusUpdate/1]).

%gen server
-export([init/1, handle_call/3, handle_info/2, terminate/2, handle_cast/2,
    code_change/3]).

-include_lib("mesos_pb.hrl").
-include_lib("mesos_erlang.hrl").

% callback specifications
-callback init( Args :: any()) -> {FrameworkInfo :: #'FrameworkInfo'{}, MasterLocation :: string(), State :: any()} | 
                                  {FrameworkInfo :: #'FrameworkInfo'{}, MasterLocation :: string(), ImplicitAcknowledgements :: boolean(), State :: any()} | 
                                  {FrameworkInfo :: #'FrameworkInfo'{}, MasterLocation :: string(), Credential :: #'Credential'{}, State :: any()} | 
                                  {FrameworkInfo :: #'FrameworkInfo'{}, MasterLocation :: string(), ImplicitAcknowledgements :: boolean(), Credential :: #'Credential'{}, State :: any()}.

-callback registered( FrameworkInfo :: #'FrameworkInfo'{}, 
                      MasterInfo :: #'MasterInfo'{},
                      State :: any()) -> {ok, State :: any()}.

-callback reregistered( MasterInfo :: #'MasterInfo'{}, State :: any()) -> {ok, State :: any()}.

-callback disconnected( State :: any()) -> {ok, State :: any()}.

-callback resourceOffers( Offer :: #'Offer'{}, State :: any()) -> {ok, State :: any()}.

-callback offerRescinded( OfferID :: #'OfferID'{}, State :: any()) -> {ok, State :: any()}.

-callback statusUpdate( TaskStatus :: #'TaskStatus'{},State :: any()) -> {ok, State :: any()}.

-callback frameworkMessage( ExecutorId :: #'ExecutorID'{},
                        SlaveId :: #'SlaveID'{},
                        Message :: string(),
                        State :: any()) -> {ok, State :: any()}.

-callback slaveLost( SlaveId :: #'SlaveID'{},State :: any()) -> {ok, State :: any()}.

-callback executorLost( ExecutorId :: #'ExecutorID'{},
                        SlaveId :: #'SlaveID'{},
                        Status :: pos_integer(),State :: any()) -> {ok, State :: any()}.

-callback error(Message :: string(),State :: any()) -> {ok, State :: any()}.   

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

-spec acceptOffers( OfferIDs :: list(#'OfferID'{}),
                    Operations :: list(#'Offer.Operation'{})) -> 
                      {ok, driver_running } 
                    | {error, scheduler_not_inited} 
                    | {error, {invalid_or_corrupted_parameter, offerid_array}}
                    | {error, {invalid_or_corrupted_parameter, operations_array}}
                    | {error, driver_state()}.
acceptOffers(OfferIDs, Operations) ->
  nif_scheduler:acceptOffers(OfferIDs, Operations).

-spec acceptOffers( OfferIDs :: list(#'OfferID'{}),
                    Operations :: list(#'Offer.Operation'{}),
                    Filters :: #'Filters'{}) ->  
                      {ok, driver_running } 
                    | {error, scheduler_not_inited} 
                    | {error, {invalid_or_corrupted_parameter, offerid_array}}
                    | {error, {invalid_or_corrupted_parameter, operations_array}}
                    | {error, {invalid_or_corrupted_parameter, filters}}
                    | {error, driver_state()}.
acceptOffers(OfferIDs, Operations, Filters) ->
  nif_scheduler:acceptOffers(OfferIDs, Operations, Filters).

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

    case whereis(?MODULE) of
        undefined  -> ok;
        _ -> unregister(?MODULE)
    end,

    Response.

%% -----------------------------------------------------------------------------------------

-spec acknowledgeStatusUpdate(TaskStatus :: #'TaskStatus'{}) ->
                                 {ok, driver_running }
                                | {error, scheduler_not_inited} 
                                | {error, {invalid_or_corrupted_parameter, task_status}}
                                | {error, driver_state()}. 
acknowledgeStatusUpdate( TaskStatus ) when is_record(TaskStatus, 'TaskStatus') ->
    nif_scheduler:acknowledgeStatusUpdate(TaskStatus).

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
             {FrameworkInfo, MasterLocation, ImplicitAcknowledgements, State} when is_record(FrameworkInfo, 'FrameworkInfo'), 
                                                                                is_list(MasterLocation),
                                                                                is_boolean(ImplicitAcknowledgements) ->
                                                 
                    ok = nif_scheduler:init(self(), FrameworkInfo, MasterLocation, ImplicitAcknowledgements), 
                    {ok,driver_running} = nif_scheduler:start(),                                    
                    {ok, #state{
                                handler_module = Module,
                                handler_state = State
                            }};
             {FrameworkInfo, MasterLocation, State} when is_record(FrameworkInfo, 'FrameworkInfo'), 
                                                         is_list(MasterLocation) ->
                    ok = nif_scheduler:init(self(), FrameworkInfo, MasterLocation, true), 
                    {ok,driver_running} = nif_scheduler:start(),                                    
                    {ok, #state{
                                handler_module = Module,
                                handler_state = State
                            }};
             {FrameworkInfo, MasterLocation, ImplicitAcknowledgements, Credential, State} when is_record(FrameworkInfo, 'FrameworkInfo'), 
                                                                is_list(MasterLocation),
                                                                is_boolean(ImplicitAcknowledgements),
                                                                is_record(Credential, 'Credential') ->
                    ok = nif_scheduler:init(self(), FrameworkInfo, MasterLocation, ImplicitAcknowledgements, Credential),
                    {ok,driver_running} = nif_scheduler:start(),  
                    {ok, #state{
                                handler_module = Module,
                                handler_state = State
                            }};
             {FrameworkInfo, MasterLocation, Credential, State} when is_record(FrameworkInfo, 'FrameworkInfo'), 
                                                                is_record(Credential, 'Credential'),
                                                                is_list(MasterLocation) ->
                    ok = nif_scheduler:init(self(), FrameworkInfo, MasterLocation, true, Credential),
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

    {ok, State1} = Module:registered(FrameworkId, MasterInfo2, HandlerState),
    {noreply, #state{ handler_module = Module, handler_state = State1 }};

handle_info({resourceOffers, OfferBin}, #state{ handler_module = Module, handler_state = HandlerState }) ->

    Offer = mesos_pb:decode_msg(OfferBin, 'Offer'),
    {ok, State1} = Module:resourceOffers(Offer, HandlerState),
    {noreply, #state{ handler_module = Module, handler_state = State1 }};

handle_info({reregistered, MasterInfoBin}, #state{ handler_module = Module, handler_state = HandlerState }) ->

    MasterInfo = mesos_pb:decode_msg(MasterInfoBin, 'MasterInfo'),
    MasterInfo2 = MasterInfo#'MasterInfo'{ip = int_to_ip(MasterInfo#'MasterInfo'.ip)},
    {ok, State1} = Module:reregistered(MasterInfo2, HandlerState),
    {noreply, #state{ handler_module = Module, handler_state = State1 }};

handle_info({disconnected}, #state{ handler_module = Module, handler_state = HandlerState }) ->

    {ok, State1} = Module:disconnected(HandlerState),
    {noreply, #state{ handler_module = Module, handler_state = State1 }};

handle_info({offerRescinded, OfferIdBin}, #state{ handler_module = Module, handler_state = HandlerState }) ->
    OfferId = mesos_pb:decode_msg(OfferIdBin, 'OfferID'),
    {ok, State1} = Module:offerRescinded(OfferId, HandlerState),
    {noreply, #state{ handler_module = Module, handler_state = State1 }};

handle_info({statusUpdate, TaskStatusBin}, #state{ handler_module = Module, handler_state = HandlerState }) ->
    TaskStatus = mesos_pb:decode_msg(TaskStatusBin, 'TaskStatus'),
    {ok, State1} = Module:statusUpdate(TaskStatus, HandlerState),
    {noreply, #state{ handler_module = Module, handler_state = State1 }};

handle_info({frameworkMessage, ExecutorIdBin, SlaveIdBin, Message}, #state{ handler_module = Module, handler_state = HandlerState }) ->
    ExecutorId = mesos_pb:decode_msg(ExecutorIdBin, 'ExecutorID'),
    SlaveId = mesos_pb:decode_msg(SlaveIdBin, 'SlaveID'),
    {ok, State1} = Module:frameworkMessage(ExecutorId, SlaveId, Message, HandlerState),
    {noreply, #state{ handler_module = Module, handler_state = State1 }};

handle_info({slaveLost, SlaveIdBin}, #state{ handler_module = Module, handler_state = HandlerState }) ->
    SlaveId = mesos_pb:decode_msg(SlaveIdBin, 'SlaveID'),
    {ok, State1} = Module:slaveLost(SlaveId, HandlerState),
    {noreply, #state{ handler_module = Module, handler_state = State1 }};

handle_info({executorLost, ExecutorIdBin, SlaveIdBin, Status}, #state{ handler_module = Module, handler_state = HandlerState }) ->
    ExecutorId = mesos_pb:decode_msg(ExecutorIdBin, 'ExecutorID'),
    SlaveId = mesos_pb:decode_msg(SlaveIdBin, 'SlaveID'),
    {ok, State1} = Module:executorLost(ExecutorId, SlaveId, Status, HandlerState),
    {noreply, #state{ handler_module = Module, handler_state = State1 }};

handle_info({error, Message}, #state{ handler_module = Module, handler_state = HandlerState }) ->
    {ok, State1} = Module:error(Message, HandlerState),
    {noreply, #state{ handler_module = Module, handler_state = State1 }}.

terminate(_Reason, _) ->
    do_terminate(),
    ok.

code_change(_, State, _) ->
  {ok, State}.

% helpers
int_to_ip(Ip)-> {Ip bsr 24, (Ip band 16711680) bsr 16, (Ip band 65280) bsr 8, Ip band 255}.

do_terminate() -> 
    scheduler:stop(0),
    scheduler:destroy().
