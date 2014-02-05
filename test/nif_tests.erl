-module (nif_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("include/mesos.hrl").
-define(setup(F), {setup, fun start/0, fun stop/1, F}).

start() ->
	ok.
stop(_) ->
	ok.

run_nif_test_() ->
	[%{"call c from nif",
	%	?setup( fun call_c_from_nif/0)},
	%{"call c that calls c++ from nif",
	%	?setup( fun call_c_that_calls_cpp_from_nif/0)},
	{"integration_test - connect to Mesos master instance",
		?setup( fun integration_with_mesos/0)}%,
	%{"aaaa",
	%	?setup( fun aaaa/0)}
	].


integration_with_mesos() ->
	MasterUrl = "127.0.1.1:5050",
	Message = mesos:encode_msg( #'FrameworkInfo'{user="Mark", name="Awesome"}),
	ok = erlang_mesos:schedular_init(Message,MasterUrl),
	ok = erlang_mesos:schedular_start(),
	timer:sleep(2000).
	
%aaaa()->
%	erlang_mesos:aaaa().

%call_c_from_nif() ->
%	hello = erlang_mesos:hello_from_c().

%call_c_that_calls_cpp_from_nif() ->
%	1 = erlang_mesos:hello_from_cpp().
