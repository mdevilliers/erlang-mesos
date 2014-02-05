-module (nif_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("include/mesos.hrl").
-define(setup(F), {setup, fun start/0, fun stop/1, F}).

start() ->
	ok.
stop(_) ->
	ok.

run_nif_test_() ->
	[
	{"integration_test - connect to Mesos master instance",
		?setup( fun integration_with_mesos/0)}
	].

integration_with_mesos() ->
	Master = "127.0.1.1:5050",
	Message = mesos:encode_msg( #'FrameworkInfo'{user="Mark", name="Awesome"}),
	ok = erlang_mesos:scheduler_init(Message,Master),
	ok = erlang_mesos:scheduler_start(),
	timer:sleep(2000).