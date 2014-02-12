-module (erlang_mesos).

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

scheduler_init(A, B, C, D)->
    nif_scheduler_init(A, B, C, D).
scheduler_init(A, B, C)->
    nif_scheduler_init(A, B, C).
scheduler_start() ->
    nif_scheduler_start().
scheduler_join() ->
    nif_scheduler_join().
scheduler_abort() ->
   nif_scheduler_abort().
scheduler_stop(A) ->
    nif_scheduler_stop(A).
scheduler_declineOffer(A,B)->
    nif_scheduler_declineOffer(A,B).
scheduler_killTask(A) ->
    nif_scheduler_killTask(A).
scheduler_reviveOffers() ->
    nif_scheduler_reviveOffers().
scheduler_sendFrameworkMessage(A,B,C) ->
    nif_scheduler_sendFrameworkMessage(A,B,C).

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
