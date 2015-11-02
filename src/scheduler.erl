-module (scheduler).

-behaviour(gen_server).

-include_lib("scheduler_pb.hrl").

-export ([start_link/0, subscribe/0, subscribe/1, subscribe/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record (state, { }).

start_link() ->
	application:ensure_started(inets),
	gen_server:start({local, ?MODULE}, ?MODULE, [], []).

% testing from cli
subscribe() ->
	subscribe(#'mesos.v1.FrameworkInfo'{user = "mark", name = "oh yeah"}).

subscribe(FrameworkInfo) when is_record(FrameworkInfo, 'mesos.v1.FrameworkInfo') ->
	subscribe(FrameworkInfo, false).	
subscribe(FrameworkInfo, Force) when is_record(FrameworkInfo, 'mesos.v1.FrameworkInfo') ,
									  is_boolean(Force) ->	
	Message = #'mesos.v1.scheduler.Call.Subscribe'{ 
		framework_info = FrameworkInfo, force = Force
	},

	gen_server:call(?MODULE, {subscribe, Message}).

init([]) ->
  	{ok, #state{}}.

handle_call({subscribe, Message}, _From, State) when is_record(Message, 'mesos.v1.scheduler.Call.Subscribe') ->

	Call = #'mesos.v1.scheduler.Call'{
		subscribe = Message,
		type = scheduler_pb:enum_symbol_by_value('mesos.v1.scheduler.Call.Type', 1)
	},

	Method = post,
	URL = "http://localhost:5050/api/v1/scheduler",
	Header = [{"Accept", "application/x-protobuf"}],
	Type = "application/x-protobuf",
	Body = scheduler_pb:encode_msg(Call),
	HTTPOptions = [],
	Options = [{sync, false}, {stream, self}],
	{ok, R}= httpc:request(Method, {URL, Header, Type, Body}, HTTPOptions, Options),
	% TODO : R goes in state
	{reply, {ok,R}, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info({http, {_, stream_start, Headers}},State) ->
	io:format("stream_start ~p~n", [Headers]), 
	{noreply, State};
handle_info({http, {_, stream_end, Headers}},State) ->
	io:format("stream_end ~p~n", [Headers]), 
	{noreply, State};
handle_info({http, {_, stream, BinBodyPart}}, State) ->
	% io:format("S : ~p~n", [parse_bits(BinBodyPart)]),
	Events = parse_recordio(BinBodyPart),

	io:format("R : ~p~n", [Events]),
	io:format("E : ~p~n", [to_events(Events)]),

    {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

% private 
split_on_length(<<>>, _) -> eof;
split_on_length(Bin, Length) ->
	<<A:Length/binary, Rest/binary>> = Bin,
	{A,Rest}.

parse_recordio(<<>>)-> [];
parse_recordio(Bin) when is_binary(Bin) ->
	[Len, Bin2] = binary:split(Bin, <<"\n">>), % split on 0xA
	case split_on_length(Bin2, binary_to_integer(Len)) of
		{Protobuf,Rest} ->
			[Protobuf| parse_recordio(Rest)];
		eof -> ok
	end.

to_events([]) -> [];
to_events([H|T]) ->
	Response = scheduler_pb:decode_msg(H, 'mesos.v1.scheduler.Event'),
	[Response | to_events(T)].
