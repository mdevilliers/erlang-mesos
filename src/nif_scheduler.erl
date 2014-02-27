-module (nif_scheduler).

-include_lib("include/mesos.hrl").

-export ([  init/4,
            init/3,
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

-on_load(init/0).

-define(APPNAME, nif_scheduler).
-define(LIBNAME, scheduler).

init(Pid, FrameworkInfo, MasterLocation, Credential) when is_pid(Pid), 
                                                            is_record(FrameworkInfo, 'FrameworkInfo'), 
                                                            is_list(MasterLocation),
                                                            is_record(Credential,'Credential')->
    nif_scheduler_init(Pid, mesos_pb:encode_msg(FrameworkInfo), MasterLocation, mesos_pb:encode_msg(Credential)).

init(Pid, FrameworkInfo, MasterLocation) when is_pid(Pid), 
                                                is_record(FrameworkInfo, 'FrameworkInfo'), 
                                                is_list(MasterLocation)->
    io:format("in scheduler init ~n", []),
    nif_scheduler_init(Pid, mesos_pb:encode_msg(FrameworkInfo), MasterLocation).

start() ->
    nif_scheduler_start().

join() ->
    nif_scheduler_join().

abort() ->
    nif_scheduler_abort().

stop(Failover) when   is_integer(Failover), 
                                Failover > -1, 
                                Failover < 2 ->
    nif_scheduler_stop(Failover).

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

sendFrameworkMessage(ExecuterId,SlaveId,Data) when    is_record(ExecuterId, 'ExecutorID'),
                                                      is_record(SlaveId, 'SlaveID'),
                                                      is_list(Data)->
    nif_scheduler_sendFrameworkMessage(mesos_pb:encode_msg(ExecuterId), mesos_pb:encode_msg(SlaveId), Data).

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


% nif functions
nif_scheduler_init(_, _, _,_)->
    not_loaded(?LINE).
nif_scheduler_init(_, _,_)->
    not_loaded(?LINE).
nif_scheduler_start() ->
    not_loaded(?LINE).
nif_scheduler_join() ->
    not_loaded(?LINE).
nif_scheduler_abort() ->
    not_loaded(?LINE).
nif_scheduler_stop(_) ->
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
encode_array([], Acc) -> Acc;
encode_array([H|T], Acc) -> 
    encode_array(T, [mesos_pb:encode_msg(H) | Acc]).