%%%-------------------------------------------------------------------
%% @doc erlang-mesos public API
%% @end
%%%-------------------------------------------------------------------

-module('erlang-mesos_app').

-behaviour(application).

%% Application callbacks
-export([start/2
        ,stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    'erlang-mesos_sup':start_link().

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
