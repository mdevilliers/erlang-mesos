-module (test_harness).

-include_lib("include/mesos.hrl").

-export ([start/0, flush/0, stop/0, declineOffer/1]).

start()->

	Master = "127.0.1.1:5050",
	Message = #'FrameworkInfo'{user="Mark", name="xxxx"},
	Pid = self(),

	ok = erlang_mesos:scheduler_init( Pid, Message, Master),
	{ok,_Status1} = erlang_mesos:scheduler_start().

stop() ->
	{ok,_Status} = erlang_mesos:scheduler_stop(1).

declineOffer(OfferIdentifer) ->
	OfferId = #'OfferID'{value=OfferIdentifer},
	Filters = #'Filters'{refuse_seconds=5},
	{ok,_Status} = erlang_mesos:scheduler_declineOffer( OfferId, Filters).

flush() ->
	receive		
		{registered , FrameworkIdBin, MasterInfoBin } ->
				FrameworkId = mesos:decode_msg(FrameworkIdBin, 'FrameworkID'),
				MasterInfo = mesos:decode_msg(MasterInfoBin, 'MasterInfo'),

				io:format("registered : FrameworkId : ~p  MasterInfo : ~p ~n", [FrameworkId, MasterInfo]),

				Ip = int_to_ip(MasterInfo#'MasterInfo'.ip),
				io:format("Master ip :~p ~n", [Ip]),
				
				flush();
		{resourceOffers, OfferBin} ->
				Offer = mesos:decode_msg(OfferBin, 'Offer'),

				io:format("resourceOffers : Offer : ~p ~n", [Offer]),				
				flush();

		Any ->
			io:format("other message from nif : ~p~n", [Any]),
			flush()
	after
		1000 ->
			ok
	end.

% helpers
int_to_ip(Ip)->	{Ip bsr 24, (Ip band 16711680) bsr 16, (Ip band 65280) bsr 8, Ip band 255}.



