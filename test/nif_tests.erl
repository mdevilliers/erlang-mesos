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
	Message = mesos:encode_msg( #'FrameworkInfo'{user="Mark", name="xxxx"}),

	%optional credential
	%Credentials = mesos:encode_msg( #'Credential'{principal="Mark", secret= <<"abc">> }),
	
	Pid = self(),

	ok = erlang_mesos:scheduler_init( Pid, Message, Master),
	{ok,_Status1} = erlang_mesos:scheduler_start(),
	
	timer:sleep(1000),

	{ok,_Status2} = erlang_mesos:scheduler_stop(1),

	flush().


int_to_ip(Ip)->	{Ip bsr 24, (Ip band 16711680) bsr 16, (Ip band 65280) bsr 8, Ip band 255}.

flush() ->
	receive
		
		{registered , FrameworkIdBin, MasterInfoBin } ->
				FrameworkId = mesos:decode_msg(FrameworkIdBin, 'FrameworkID'),
				MasterInfo = mesos:decode_msg(MasterInfoBin, 'MasterInfo'),

				?debugFmt("registered : FrameworkId : ~p  MasterInfo : ~p ~n", [FrameworkId, MasterInfo]),

				Ip = int_to_ip(MasterInfo#'MasterInfo'.ip),
				?debugFmt("Master ip :~p ~n", [Ip]),
				
				flush();
		Any ->
			?debugFmt("other message from nif : ~p~n", [Any]),
			flush()
	after
		1000 ->
			ok
	end.