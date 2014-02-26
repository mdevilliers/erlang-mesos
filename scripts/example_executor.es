#! /usr/bin/env escript
%%
%% Executable "executor"
%%

main(_) ->

    io:format("~p", [file:get_cwd()]),
    true = code:add_pathz( "ebin"),
    example_executor:init(),

    timer:sleep(5000).