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

%api
-export([init/4,
        init/5,
        start/0,
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

% private
-export ([loop/2]).

-include_lib("mesos_pb.hrl").
-include_lib("mesos_erlang.hrl").

% callback specifications
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

% implementation
-spec init(Module :: module(), FrameworkInfo :: #'FrameworkInfo'{}, MasterLocation :: string(), State :: any()) ->  
                          {state_error, scheduler_already_inited}
                        | {argument_error, invalid_or_corrupted_parameter, pid }
                        | {argument_error, invalid_or_corrupted_parameter, framework_info}
                        | {argument_error, invalid_or_corrupted_parameter, master_info}
                        | ok.

init(Module, FrameworkInfo, MasterLocation, State) when is_record(FrameworkInfo, 'FrameworkInfo'), 
                                                        is_list(MasterLocation) ->
    Pid = spawn(?MODULE, loop, [Module, State]),
    register(scheduler_loop, Pid),
    nif_scheduler:init(Pid, FrameworkInfo, MasterLocation).

%% -----------------------------------------------------------------------------------------

-spec init(Module :: module(), FrameworkInfo :: #'FrameworkInfo'{},MasterLocation :: string(),Credential :: #'Credential'{},State :: any()) ->  
                          {state_error, scheduler_already_inited}
                        | {argument_error, invalid_or_corrupted_parameter, pid }
                        | {argument_error, invalid_or_corrupted_parameter, framework_info}
                        | {argument_error, invalid_or_corrupted_parameter, master_info}
                        | {argument_error, invalid_or_corrupted_parameter, credential}
                        | ok.
init(Module, FrameworkInfo, MasterLocation, Credential, State) when is_record(FrameworkInfo, 'FrameworkInfo'), 
                                                        is_list(MasterLocation) ->
    Pid = spawn(?MODULE, loop, [Module, State]),
    register(scheduler_loop, Pid),
    nif_scheduler:init(Pid, FrameworkInfo, MasterLocation, Credential).

%% -----------------------------------------------------------------------------------------

-spec start() -> { state_error, scheduler_not_inited} | {ok, driver_running } | {error, driver_state()}.
start() ->
    nif_scheduler:start().

%% -----------------------------------------------------------------------------------------


-spec join() -> { state_error, scheduler_not_inited} | {ok, driver_running } | {error, driver_state()}.
join() ->
    nif_scheduler:join().

%% -----------------------------------------------------------------------------------------


-spec abort() -> { state_error, scheduler_not_inited} | {ok, driver_aborted } | {error, driver_state()}.
abort() ->
    nif_scheduler:abort().

%% -----------------------------------------------------------------------------------------

-spec stop(integer()) -> { state_error, scheduler_not_inited} | {ok, driver_stopped } | {error, driver_state()}.    
stop(Failover) when is_integer(Failover), 
                                Failover > -1, 
                                Failover < 2 ->
    nif_scheduler:stop(Failover).

%% -----------------------------------------------------------------------------------------

-spec declineOffer( OfferId :: #'OfferID'{}) 
                    -> { state_error, scheduler_not_inited} 
                    | {ok, driver_running } 
                    | {argument_error, invalid_or_corrupted_parameter, offer_id}
                    | {error, driver_state()}.


declineOffer(OfferId) when is_record(OfferId, 'OfferID') ->
    nif_scheduler:declineOffer(OfferId). 

-spec declineOffer( OfferId :: #'OfferID'{},
                    Filter :: #'Filters'{}) 
                    -> { state_error, scheduler_not_inited} 
                    | {ok, driver_running } 
                    | {argument_error, invalid_or_corrupted_parameter, offer_id}
                    | {argument_error, invalid_or_corrupted_parameter, filters}
                    | {error, driver_state()}.

declineOffer(OfferId,Filter) when is_record(OfferId, 'OfferID'),
                                  is_record(Filter, 'Filters') ->
    nif_scheduler:declineOffer(OfferId,Filter).

%% -----------------------------------------------------------------------------------------

-spec killTask( TaskId :: #'TaskID'{}) 
                    -> { state_error, scheduler_not_inited} 
                    | {ok, driver_running } 
                    | {argument_error, invalid_or_corrupted_parameter, task_id}
                    | {error, driver_state()}.

killTask(TaskId) when is_record(TaskId,'TaskID') ->
    nif_scheduler:killTask(TaskId).

%% -----------------------------------------------------------------------------------------

-spec reviveOffers() -> { state_error, scheduler_not_inited} | {ok, driver_aborted } | {error, driver_state()}.

reviveOffers()->
    nif_scheduler:reviveOffers().

%% -----------------------------------------------------------------------------------------

-spec sendFrameworkMessage( ExecutorId :: #'ExecutorID'{},
                            SlaveId :: #'SlaveID'{},
                            Data ::string()) 
                            -> { state_error, scheduler_not_inited} 
                            | {ok, driver_running } 
                            | {argument_error, invalid_or_corrupted_parameter, executor_id}
                            | {argument_error, invalid_or_corrupted_parameter, slave_id}
                            | {argument_error, invalid_or_corrupted_parameter, data}
                            | {error, driver_state()}.

sendFrameworkMessage(ExecutorId,SlaveId,Data) when is_record(ExecutorId, 'ExecutorID'),
                                                   is_record(SlaveId, 'SlaveID'),
                                                   is_list(Data)->
    nif_scheduler:sendFrameworkMessage(ExecutorId,SlaveId,Data).

%% -----------------------------------------------------------------------------------------

-spec requestResources( Requests :: [ #'Request'{} ]) 
                    -> { state_error, scheduler_not_inited} 
                    | {ok, driver_running } 
                    | {argument_error, invalid_or_corrupted_parameter, request_array}
                    | {error, driver_state()}.

requestResources(Requests) when is_list(Requests) ->
    nif_scheduler:requestResources(Requests).

%% -----------------------------------------------------------------------------------------

-spec reconcileTasks( TaskStatus :: [ #'TaskStatus'{} ]) 
                    -> { state_error, scheduler_not_inited} 
                    | {ok, driver_running } 
                    | {argument_error, invalid_or_corrupted_parameter, task_status_array}
                    | {error, driver_state()}.

reconcileTasks(TaskStatus)when is_list(TaskStatus)->
    nif_scheduler:reconcileTasks(TaskStatus).

%% -----------------------------------------------------------------------------------------

-spec launchTasks(  OfferId :: #'OfferID'{}, 
                    TaskInfos :: [ #'TaskInfo'{}]) 
                    -> { state_error, scheduler_not_inited} 
                    | {ok, driver_running } 
                    | {argument_error, invalid_or_corrupted_parameter, offer_id}
                    | {argument_error, invalid_or_corrupted_parameter, task_info_array}
                    | {error, driver_state()}.

launchTasks(OfferId, TaskInfos) when is_record(OfferId, 'OfferID'), 
                                     is_list(TaskInfos) ->
    nif_scheduler:launchTasks(OfferId, TaskInfos).

-spec launchTasks(  OfferId :: #'OfferID'{}, 
                    TaskInfos :: [ #'TaskInfo'{}],
                    Filter :: #'Filters'{}) 
                    -> { state_error, scheduler_not_inited} 
                    | {ok, driver_running } 
                    | {argument_error, invalid_or_corrupted_parameter, offer_id}
                    | {argument_error, invalid_or_corrupted_parameter, task_info_array}
                    | {argument_error, invalid_or_corrupted_parameter, filters}
                    | {error, driver_state()}.

launchTasks(OfferId, TaskInfos, Filter) when is_record(OfferId, 'OfferID'), 
                                             is_list(TaskInfos),
                                             is_record(Filter, 'Filters') ->
    nif_scheduler:launchTasks(OfferId, TaskInfos, Filter).

%% -----------------------------------------------------------------------------------------

-spec destroy() -> ok | {state_error, scheduler_not_inited}.
destroy() ->
    case nif_scheduler:destroy() of
        ok ->
            scheduler_loop ! {internal_shudown},
            ok;
        Other ->
            Other
    end.
%% -----------------------------------------------------------------------------------------

% main call back loop
loop(Module,State) -> 
    receive     
        {registered , FrameworkIdBin, MasterInfoBin } ->            
                FrameworkId = mesos_pb:decode_msg(FrameworkIdBin, 'FrameworkID'),
                MasterInfo = mesos_pb:decode_msg(MasterInfoBin, 'MasterInfo'),
                MasterInfo2 = MasterInfo#'MasterInfo'{ip = int_to_ip(MasterInfo#'MasterInfo'.ip)},
                {ok, State1} = Module:registered(State, FrameworkId, MasterInfo2),
                loop(Module, State1);
        {resourceOffers, OfferBin} ->
                Offer = mesos_pb:decode_msg(OfferBin, 'Offer'),
                 {ok, State1} = Module:resourceOffers(State,Offer),
                loop(Module, State1);
        {reregistered, MasterInfoBin} ->
                MasterInfo = mesos_pb:decode_msg(MasterInfoBin, 'MasterInfo'),
                MasterInfo2 = MasterInfo#'MasterInfo'{ip = int_to_ip(MasterInfo#'MasterInfo'.ip)},
                {ok, State1} = Module:reregistered(State,MasterInfo2),
                loop(Module,State1);
        {disconnected} ->
                {ok, State1} = Module:disconnected(State),
                loop(Module, State1);   
        {offerRescinded, OfferIdBin} ->
                OfferId = mesos_pb:decode_msg(OfferIdBin, 'OfferID'),
                {ok, State1} = Module:offerRescinded(State,OfferId),
                loop(Module,State1);
        {statusUpdate, TaskStatusBin} ->
                TaskStatus = mesos_pb:decode_msg(TaskStatusBin, 'TaskStatus'),
                {ok, State1} = Module:statusUpdate(State,TaskStatus),
                loop(Module,State1);
        {frameworkMessage, ExecutorIdBin, SlaveIdBin, Message} ->
                ExecutorId = mesos_pb:decode_msg(ExecutorIdBin, 'ExecutorID'),
                SlaveId = mesos_pb:decode_msg(SlaveIdBin, 'SlaveID'),
                {ok, State1} = Module:frameworkMessage(State,ExecutorId,SlaveId,Message),
                loop(Module, State1);
        {slaveLost, SlaveIdBin} ->
                SlaveId = mesos_pb:decode_msg(SlaveIdBin, 'SlaveID'),
                {ok, State1} = Module:frameworkMessage(State,SlaveId),
                loop(Module,State1);
        {executorLost, ExecutorIdBin, SlaveIdBin, Status} ->
                ExecutorId = mesos_pb:decode_msg(ExecutorIdBin, 'ExecutorID'),
                SlaveId = mesos_pb:decode_msg(SlaveIdBin, 'SlaveID'),
                {ok, State1} = Module:executorLost(State, ExecutorId,SlaveId,Status),
                loop(Module,State1);
        {error, Message} ->
                {ok, State1} = Module:error(State,Message),
                loop(Module,State1);   
        {internal_shudown} ->
                unregister(scheduler_loop),
                {shutdown_complete};        
        Any ->
            io:format("SCHEDULER: UNKNOWN MESSAGE : ~p~n", [Any]),
            loop(Module,State)
    end.

% helpers
int_to_ip(Ip)-> {Ip bsr 24, (Ip band 16711680) bsr 16, (Ip band 65280) bsr 8, Ip band 255}.
