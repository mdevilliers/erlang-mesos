-module (erlang_mesos).

-export([hello/0,hello_from_cpp/0]).
-on_load(init/0).

-define(APPNAME, erlang_mesos).
-define(LIBNAME, erlang_mesos).

hello() ->
    not_loaded(?LINE).

hello_from_cpp() ->
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