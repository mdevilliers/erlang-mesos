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


-module (nif_scheduler).

-include_lib("mesos_pb.hrl").

-export ([  init/5,
            init/4,
            start/0,
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

-on_load(init/0).

-define(APPNAME, erlang_mesos).
-define(LIBNAME, scheduler).

init(Pid, FrameworkInfo, MasterLocation, ImplicitAcknowledgements, Credential) when is_pid(Pid), 
                                                            is_record(FrameworkInfo, 'FrameworkInfo'), 
                                                            is_list(MasterLocation),
                                                            is_boolean(ImplicitAcknowledgements),
                                                            is_record(Credential,'Credential')->
    nif_scheduler_init(Pid, mesos_pb:encode_msg(FrameworkInfo), MasterLocation, bool_to_int(ImplicitAcknowledgements), mesos_pb:encode_msg(Credential)).

init(Pid, FrameworkInfo, MasterLocation, ImplicitAcknowledgements) when is_pid(Pid), 
                                                is_record(FrameworkInfo, 'FrameworkInfo'), 
                                                is_list(MasterLocation),
                                                is_boolean(ImplicitAcknowledgements)->
    nif_scheduler_init(Pid, mesos_pb:encode_msg(FrameworkInfo), MasterLocation, bool_to_int(ImplicitAcknowledgements)).

start() ->
    nif_scheduler_start().

join() ->
    nif_scheduler_join().

abort() ->
    nif_scheduler_abort().

stop(Failover) when is_integer(Failover), 
                                Failover > -1, 
                                Failover < 2 ->
    nif_scheduler_stop(Failover).

acceptOffers(OfferIDs, Operations) when is_list(OfferIDs), 
                                        is_list(Operations) ->
  acceptOffers(OfferIDs, Operations, #'Filters'{}).
acceptOffers(OfferIDs, Operations, Filters) when is_list(OfferIDs), 
                                                 is_list(Operations),
                                                 is_record(Filters, 'Filters') ->
    nif_scheduler_acceptOffers( encode_array(OfferIDs, []), encode_array(Operations, []), mesos_pb:encode_msg(Filters)).

declineOffer(OfferId) when is_record(OfferId, 'OfferID') ->
    Filter = #'Filters'{},
    nif_scheduler_declineOffer(mesos_pb:encode_msg(OfferId), mesos_pb:encode_msg(Filter)).

declineOffer(OfferId,Filter) when is_record(OfferId, 'OfferID'),
                                            is_record(Filter, 'Filters') ->
    nif_scheduler_declineOffer(mesos_pb:encode_msg(OfferId), mesos_pb:encode_msg(Filter)).

killTask(TaskId) when is_record(TaskId,'TaskID')->
    nif_scheduler_killTask(mesos_pb:encode_msg(TaskId)).

reviveOffers() ->
    nif_scheduler_reviveOffers().

sendFrameworkMessage(ExecutorId,SlaveId,Data) when    is_record(ExecutorId, 'ExecutorID'),
                                                      is_record(SlaveId, 'SlaveID'),
                                                      is_list(Data)->
    nif_scheduler_sendFrameworkMessage(mesos_pb:encode_msg(ExecutorId), mesos_pb:encode_msg(SlaveId), Data).

requestResources(Requests) when is_list(Requests) ->
    EncodedRequests = encode_array(Requests, []),
    nif_scheduler_requestResources(EncodedRequests).

reconcileTasks(TaskStatuss) when is_list(TaskStatuss)->
    EncodedTaskStatus = encode_array(TaskStatuss, []),
    nif_scheduler_reconcileTasks(EncodedTaskStatus).

launchTasks(OfferId, TaskInfos ) when is_record(OfferId, 'OfferID'), 
                                        is_list(TaskInfos) ->
    EncodedTaskInfos = encode_array(TaskInfos, []),
    Filter = #'Filters'{},
    nif_scheduler_launchTasks(mesos_pb:encode_msg(OfferId), EncodedTaskInfos, mesos_pb:encode_msg(Filter)).

launchTasks(OfferId, TaskInfos, Filter ) when is_record(OfferId, 'OfferID'), 
                                              is_list(TaskInfos),
                                              is_record(Filter, 'Filters') ->
    EncodedTaskInfos = encode_array(TaskInfos, []),
    nif_scheduler_launchTasks(mesos_pb:encode_msg(OfferId), EncodedTaskInfos, mesos_pb:encode_msg(Filter)).

destroy()->
    nif_scheduler_destroy().

acknowledgeStatusUpdate(TaskStatus) when is_record(TaskStatus, 'TaskStatus') ->
    nif_scheduler_acknowledgeStatusUpdate(mesos_pb:encode_msg(TaskStatus)).

% nif functions
nif_scheduler_init(_, _, _, _, _)->
    not_loaded(?LINE).
nif_scheduler_init(_, _, _, _)->
    not_loaded(?LINE).
nif_scheduler_start() ->
    not_loaded(?LINE).
nif_scheduler_join() ->
    not_loaded(?LINE).
nif_scheduler_abort() ->
    not_loaded(?LINE).
nif_scheduler_stop(_) ->
    not_loaded(?LINE).
nif_scheduler_acceptOffers(_, _, _)->
    not_loaded(?LINE).
nif_scheduler_declineOffer(_,_)->
    not_loaded(?LINE).
nif_scheduler_killTask(_) ->
    not_loaded(?LINE).
nif_scheduler_reviveOffers() ->
    not_loaded(?LINE).
nif_scheduler_sendFrameworkMessage(_,_,_) ->
    not_loaded(?LINE).
nif_scheduler_requestResources(_) ->
    not_loaded(?LINE).
nif_scheduler_reconcileTasks(_) ->
    not_loaded(?LINE).
nif_scheduler_launchTasks(_,_,_) ->
    not_loaded(?LINE).
nif_scheduler_destroy() ->
    not_loaded(?LINE).
nif_scheduler_acknowledgeStatusUpdate(_) ->
    not_loaded(?LINE).

init() ->
    SoName = case code:priv_dir(?APPNAME) of
        {error, bad_name} ->
            case filelib:is_dir(filename:join(["..", priv])) of
                true ->
                    filename:join(["..", priv, ?LIBNAME]);
                _ ->
                    filename:join([priv, ?LIBNAME])
            end;
        Dir ->
            filename:join(Dir, ?LIBNAME)
    end,
    erlang:load_nif(SoName, 0).

not_loaded(Line) ->
    exit({not_loaded, [{module, ?MODULE}, {line, Line}]}).

% helpers
bool_to_int(true) -> 1;
bool_to_int(false) -> 0.

encode_array([], Acc) -> Acc;
encode_array([H|T], Acc) -> 
    encode_array(T, [mesos_pb:encode_msg(H) | Acc]).
