-module (scheduler).

-behaviour(gen_server).

-include_lib("scheduler_pb.hrl").

-export ([start_link/0, subscribe/0, subscribe/1, subscribe/2, teardown/1, accept/3, accept/4, decline/2, decline/3, revive/1, kill/2, kill/3, shutdown/3, acknowledge/3, reconcile/2, message/4, request/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record (scheduler_state, { master_url, persistent_connection }).

start_link(MasterUrl) ->
	application:ensure_started(inets),
	gen_server:start({local, ?MODULE}, ?MODULE, [MasterUrl], []).

start_link() ->
	start_link("http://localhost:5050/api/v1/scheduler").

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

teardown(FrameworkId) when is_record(FrameworkId, 'mesos.v1.FrameworkID') -> gen_server:call(?MODULE, {teardown, FrameworkId}).

accept(FrameworkId, OfferIds, Operations) when is_record(FrameworkId, 'mesos.v1.FrameworkID') -> gen_server:call(?MODULE, {accept, FrameworkId, OfferIds, Operations}).

accept(FrameworkId, OfferIds, Operations, Filters) when is_record(FrameworkId, 'mesos.v1.FrameworkID')
												   -> gen_server:call(?MODULE, {accept, FrameworkId, OfferIds, Operations, Filters}).


decline(FrameworkId, OfferIds) when is_record(FrameworkId, 'mesos.v1.FrameworkID')
									-> gen_server:call(?MODULE, {decline, FrameworkId, OfferIds}).

decline(FrameworkId, OfferIds, Filters) when is_record(FrameworkId, 'mesos.v1.FrameworkID')
										-> gen_server:call(?MODULE, {decline, FrameworkId, OfferIds, Filters}).

revive(FrameworkId) when is_record(FrameworkId, 'mesos.v1.FrameworkID')  
						 -> gen_server:call(?MODULE, {revive, FrameworkId}).

kill(FrameworkId, TaskId) when is_record(FrameworkId, 'mesos.v1.FrameworkID'),
							            is_record(TaskId, 'mesos.v1.TaskID')
							 			-> gen_server:call(?MODULE, {kill, FrameworkId, TaskId}).

kill(FrameworkId, TaskId, AgentId) when is_record(FrameworkId, 'mesos.v1.FrameworkID'),
							            is_record(TaskId, 'mesos.v1.TaskID'),
							            is_record(AgentId, 'mesos.v1.AgentID')
							 			-> gen_server:call(?MODULE, {kill, FrameworkId, TaskId, AgentId}).

shutdown(FrameworkId, ExecutorId, AgentId)  when is_record(FrameworkId, 'mesos.v1.FrameworkID'),
									  			 is_record(ExecutorId,'mesos.v1.ExecutorID'),
									             is_record(AgentId,'mesos.v1.AgentID') 
									  			-> gen_server:call(?MODULE, {shutdown, FrameworkId, ExecutorId, AgentId}).

acknowledge(FrameworkId, AgentId, TaskId) when is_record(FrameworkId, 'mesos.v1.FrameworkID'),
									       is_record(AgentId,'mesos.v1.AgentID'),
									       is_record(TaskId,'mesos.v1.TaskID')
									       -> gen_server:call(?MODULE, {acknowledge, FrameworkId, AgentId, TaskId}).

reconcile(FrameworkId, Tasks) when is_record(FrameworkId, 'mesos.v1.FrameworkID')
							  -> gen_server:call(?MODULE, {reconcile, FrameworkId, Tasks}).


message(FrameworkId, AgentId, ExecutorId, Message) when is_record(FrameworkId, 'mesos.v1.FrameworkID'),
														is_record(ExecutorId, 'mesos.v1.ExecutorID')
														-> gen_server:call(?MODULE, {message, FrameworkId, AgentId, ExecutorId, Message}).

request(FrameworkId, Requests) when is_record(FrameworkId, 'mesos.v1.FrameworkID')
								    -> gen_server:call(?MODULE, {request, FrameworkId, Requests}).

% gen server callbacks
init([MasterUrl]) ->
  	{ok, #scheduler_state{ master_url = MasterUrl }}.


handle_call({subscribe, Message}, _From, #scheduler_state{master_url = MasterUrl} = State) 
		when is_record(Message, 'mesos.v1.scheduler.Call.Subscribe') ->

	Call = #'mesos.v1.scheduler.Call'{
		subscribe = Message,
		type = scheduler_pb:enum_symbol_by_value('mesos.v1.scheduler.Call.Type', 1)
	},

	Method = post,
	URL = MasterUrl,
	Header = [{"Accept", "application/x-protobuf"}],
	Type = "application/x-protobuf",
	Body = scheduler_pb:encode_msg(Call),
	HTTPOptions = [],
	Options = [{sync, false}, {stream, self}],
	{ok, Request}= httpc:request(Method, {URL, Header, Type, Body}, HTTPOptions, Options),

	State1 = State#scheduler_state{persistent_connection = Request},
	{reply, ok, State1}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info({http, {_, stream_start, Headers}},State) ->
	io:format("stream_start ~p~n", [Headers]), 
	{noreply, State};
handle_info({http, {_, stream_end, Headers}},State) ->
	io:format("stream_end ~p~n", [Headers]), 
	{noreply, State};
handle_info({http, {_, stream, BinBodyPart}}, State) ->
	BinaryBits = parse_recordio(BinBodyPart),
	Events = to_events(BinaryBits),
	lists:map(fun(Event) -> dispatch_event(Event) end, Events),
    {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

% callbacks
dispatch_event(Event) -> dispatch_event(Event#'mesos.v1.scheduler.Event'.type, Event).

dispatch_event('SUBSCRIBED', Event) ->
	io:format("SUBSCRIBED : ~p~n", [Event#'mesos.v1.scheduler.Event'.subscribed]);
dispatch_event('OFFERS', Event) ->
	io:format("OFFERS : ~p~n", [Event#'mesos.v1.scheduler.Event'.offers]);
dispatch_event('RESCIND', Event) ->
	io:format("RESCIND : ~p~n", [Event#'mesos.v1.scheduler.Event'.rescind]);
dispatch_event('UPDATE', Event) ->
	io:format("UPDATE : ~p~n", [Event#'mesos.v1.scheduler.Event'.update]);
dispatch_event('MESSAGE', Event) ->
	io:format("MESSAGE : ~p~n", [Event#'mesos.v1.scheduler.Event'.message]);
dispatch_event('FAILURE', Event) ->
	io:format("FAILURE : ~p~n", [Event#'mesos.v1.scheduler.Event'.failure]);
dispatch_event('ERROR', Event) ->
	io:format("ERROR : ~p~n", [Event#'mesos.v1.scheduler.Event'.error]);
dispatch_event('HEARTBEAT', Event) ->
	io:format("HEARTBEAT : ~p~n", [Event]).

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
