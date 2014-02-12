-module (erlang_mesos).

-include_lib("include/mesos.hrl").

-export ([scheduler_init/4,
            scheduler_init/3,
            scheduler_start/0,
            scheduler_join/0,
            scheduler_abort/0,
            scheduler_stop/1,
            scheduler_declineOffer/2,
            scheduler_killTask/1,
            scheduler_reviveOffers/0,
            scheduler_sendFrameworkMessage/3]).

-on_load(init/0).

-define(APPNAME, erlang_mesos).
-define(LIBNAME, erlang_mesos).

scheduler_init(Pid, FrameworkInfo, MasterLocation, Credential) when is_pid(Pid), 
                                                            is_record(FrameworkInfo, 'FrameworkInfo'), 
                                                            is_list(MasterLocation),
                                                            is_record(Credential,'Credential')->
    nif_scheduler_init(Pid, mesos:encode_msg(FrameworkInfo), MasterLocation, mesos:encode_msg(Credential)).

scheduler_init(Pid, FrameworkInfo, MasterLocation) when is_pid(Pid), 
                                                            is_record(FrameworkInfo, 'FrameworkInfo'), 
                                                            is_list(MasterLocation)->
    nif_scheduler_init(Pid, mesos:encode_msg(FrameworkInfo), MasterLocation).

scheduler_start() ->
    nif_scheduler_start().

scheduler_join() ->
    nif_scheduler_join().

scheduler_abort() ->
    nif_scheduler_abort().

scheduler_stop(Failover) when   is_integer(Failover), 
                                Failover > -1, 
                                Failover < 2 ->
    nif_scheduler_stop(Failover).

scheduler_declineOffer(OfferId,Filter) when is_record(OfferId, 'OfferID'),
                                            is_record(Filter, 'Filters')->
    nif_scheduler_declineOffer(mesos:encode_msg(OfferId), mesos:encode_msg(Filter)).

scheduler_killTask(TaskId) when is_record(TaskId,'TaskID')->
    nif_scheduler_killTask(mesos:encode_msg(TaskId)).

scheduler_reviveOffers() ->
    nif_scheduler_reviveOffers().

scheduler_sendFrameworkMessage(ExecuterId,SlaveId,Data) when    is_record(ExecuterId, 'ExecutorID'),
                                                                is_record(SlaveId, 'SlaveID'),
                                                                is_list(Data)->
    nif_scheduler_sendFrameworkMessage(mesos:encode_msg(ExecuterId), mesos:encode_msg(SlaveId), Data).


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
