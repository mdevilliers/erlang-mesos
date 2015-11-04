-module (scheduler).

-behaviour(gen_server).

-include_lib("scheduler_pb.hrl").

-export([start/2, start_link/2, teardown/1, accept/3, accept/4, decline/2, decline/3, revive/1, kill/2, kill/3, shutdown/3, acknowledge/4, reconcile/2, message/4, request/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record (scheduler_state, { 
        master_url, 
        implicit_ackowledgments, 
        persistent_connection, 
        framework_id = undefined,
        handler_module,
        handler_state 
        }).

-define (SCHEDULER_API_URI, "/api/v1/scheduler").
-define (SCHEDULER_API_TRANSPORT, "application/x-protobuf").

% callback specifications
-type scheduler_client() :: pid().
-type offers() :: ['mesos.v1.Offer'].
-type operations() ::  ['mesos.v1.Offer.Operation'].
-type reconcile_tasks() ::  ['mesos.v1.scheduler.Call.Reconcile.Task'].
-type requests() ::  ['mesos.v1.Request'].

-callback init( Args :: any()) 
    -> {FrameworkInfo :: #'mesos.v1.FrameworkInfo'{}, MasterUrl :: string(), ImplicitAcknowledgements :: boolean(), Force :: boolean(), State :: any()}.
-callback subscribed( Client :: scheduler_client(), State :: any()) 
    -> {ok, State :: any()}.
-callback offers( Client :: scheduler_client(), Offers :: offers(), State :: any()) 
    -> {ok, State :: any()}.
-callback inverse_offers(Client :: scheduler_client(), Offers :: offers(), State :: any()) 
    -> {ok, State :: any()}.
-callback rescind( Client :: scheduler_client(), OfferId :: #'mesos.v1.OfferID'{}, State :: any()) 
    -> {ok, State :: any()}.
-callback update( Client :: scheduler_client(), TaskStatus :: #'mesos.v1.TaskStatus'{}, State :: any()) 
    -> {ok, State :: any()}.
-callback message( Client :: scheduler_client(), AgentId :: #'mesos.v1.AgentID'{}, ExecutorId :: #'mesos.v1.ExecutorID'{}, Data :: binary(), State :: any()) 
    -> {ok, State :: any()}.
-callback failure( Client :: scheduler_client(), AgentId :: #'mesos.v1.AgentID'{}, ExecutorId :: #'mesos.v1.ExecutorID'{}, Status :: number(), State :: any()) 
    -> {ok, State :: any()}.
-callback error( Client :: scheduler_client(), Message :: list(), State :: any()) 
    -> {ok, State :: any()}.

% public api
-spec start(Module :: atom(), Args :: term()) ->
    {ok, Server :: pid()} | {error, Reason :: term()}.

start(Module, Args) ->
    application:ensure_started(inets),
    gen_server:start(?MODULE, {Module, Args}, []).

-spec start_link(Module :: atom(), Args :: term()) ->
    {ok, Server :: pid()} | {error, Reason :: term()}.

start_link(Module, Args ) ->

    application:ensure_started(inets),
    gen_server:start_link(?MODULE, {Module, Args}, []).

-spec teardown(Scheduler :: scheduler_client()) -> ok.

teardown(Scheduler) when is_pid(Scheduler) -> 
    gen_server:cast(Scheduler, {teardown}).

-spec accept(Scheduler :: scheduler_client(), 
             OfferIds :: offers(), 
             Operations :: operations()) -> ok.

accept(Scheduler, OfferIds, Operations) when is_pid(Scheduler) -> 
    
    Message = #'mesos.v1.scheduler.Call.Accept'{ 
        offer_ids = OfferIds,
        operations = Operations
    },
    
    gen_server:cast(Scheduler, {accept, Message}).

-spec accept(Scheduler :: scheduler_client(), 
             OfferIds :: offers(), 
             Operations :: operations(), 
             Filters :: #'mesos.v1.Filters'{}) -> ok.

accept(Scheduler, OfferIds, Operations, Filters) when is_pid(Scheduler), is_record(Filters, 'mesos.v1.Filters') -> 
    
    Message = #'mesos.v1.scheduler.Call.Accept'{ 
        offer_ids = OfferIds,
        operations = Operations,
        filters = Filters
    },
    
    gen_server:cast(Scheduler, {accept, Message}).

-spec decline(Scheduler :: scheduler_client(), 
             OfferIds :: offers()) -> ok.

decline(Scheduler, OfferIds) when is_pid(Scheduler)-> 
    
    Message = #'mesos.v1.scheduler.Call.Decline'{ 
        offer_ids = OfferIds
    },
    
    gen_server:cast(Scheduler, {decline, Message}).

-spec decline(Scheduler :: scheduler_client(), 
             OfferIds :: offers(), 
             Filters :: #'mesos.v1.Filters'{}) -> ok.

decline(Scheduler, OfferIds, Filters) when is_pid(Scheduler), is_record(Filters, 'mesos.v1.Filters') ->
    
    Message = #'mesos.v1.scheduler.Call.Decline'{ 
        offer_ids = OfferIds,
        filters = Filters
    },
    
    gen_server:cast(Scheduler, {decline, Message}).

-spec revive(Scheduler :: scheduler_client()) -> ok.

revive(Scheduler) when is_pid(Scheduler)  -> 
    gen_server:cast(Scheduler, {revive}).

-spec kill (Scheduler :: scheduler_client(), 
            TaskId :: #'mesos.v1.TaskID'{} ) -> ok.

kill(Scheduler, TaskId) when is_pid(Scheduler), is_record(TaskId, 'mesos.v1.TaskID') -> 

    Message = #'mesos.v1.scheduler.Call.Kill'{ 
            task_id = TaskId
    },

    gen_server:cast(Scheduler, {kill, Message}).

-spec kill (Scheduler :: scheduler_client(), 
            TaskId :: #'mesos.v1.TaskID'{},
            AgentId :: #'mesos.v1.AgentID'{} ) -> ok.

kill(Scheduler, TaskId, AgentId) when is_pid(Scheduler), is_record(TaskId, 'mesos.v1.TaskID'), is_record(AgentId, 'mesos.v1.AgentID') -> 

    Message = #'mesos.v1.scheduler.Call.Kill'{ 
            task_id = TaskId,
            agent_id = AgentId
        },

    gen_server:cast(Scheduler, {kill, Message}).

-spec shutdown(Scheduler :: scheduler_client(),
               ExecutorId :: #'mesos.v1.ExecutorID'{},
               AgentId :: #'mesos.v1.AgentID'{}) -> ok.

shutdown(Scheduler, ExecutorId, AgentId) when is_pid(Scheduler), is_record(ExecutorId,'mesos.v1.ExecutorID'), is_record(AgentId,'mesos.v1.AgentID') -> 

    Message = #'mesos.v1.scheduler.Call.Shutdown'{ 
            executor_id = ExecutorId,
            agent_id = AgentId
        },

    gen_server:cast(Scheduler, {shutdown, Message}).

-spec acknowledge(Scheduler :: scheduler_client(),
               AgentId :: #'mesos.v1.AgentID'{},
               TaskId :: #'mesos.v1.TaskID' {},
               Uuid :: [byte()]) -> ok.

acknowledge(Scheduler, AgentId, TaskId, Uuid) when is_pid(Scheduler), is_record(AgentId,'mesos.v1.AgentID'), is_record(TaskId,'mesos.v1.TaskID') -> 

    Message = #'mesos.v1.scheduler.Call.Acknowledge'{ 
        agent_id = AgentId, 
        task_id = TaskId,
        uuid = Uuid
    },
    gen_server:cast(Scheduler, {acknowledge, Message}).

-spec reconcile(Scheduler :: scheduler_client(),
                Tasks :: reconcile_tasks()) -> ok.

reconcile(Scheduler, Tasks) when is_pid(Scheduler) -> 

    Message = #'mesos.v1.scheduler.Call.Reconcile'{ 
        tasks = Tasks
    },
    gen_server:cast(Scheduler, {reconcile, Message}).

-spec message(Scheduler :: scheduler_client(),
               AgentId :: #'mesos.v1.AgentID'{},
               ExecutorId :: #'mesos.v1.ExecutorID'{},
               Data :: [byte()]) -> ok.

message(Scheduler, AgentId, ExecutorId, Data) when is_pid(Scheduler), is_record(AgentId, 'mesos.v1.AgentID'), is_record(ExecutorId, 'mesos.v1.ExecutorID')-> 
    Message = #'mesos.v1.scheduler.Call.Message'{ 
        agent_id = AgentId,
        executor_id = ExecutorId,
        data = Data
    },

    gen_server:cast(Scheduler, {message, Message}).

-spec request(Scheduler :: scheduler_client(),
               Requests :: requests()) -> ok.

request(Scheduler, Requests) when is_pid(Scheduler) -> 
    
    Message = #'mesos.v1.scheduler.Call.Request'{ 
        requests = Requests
    },

    gen_server:cast(Scheduler, {request, Message}).

% gen server callbacks
init({Module, Args}) ->
    
    case Module:init(Args) of 
        { FrameworkInfo, MasterUrl, ImplicitAcknowledgements, Force, State } when is_record(FrameworkInfo, 'mesos.v1.FrameworkInfo'), 
                                                                                is_list(MasterUrl),
                                                                                is_boolean(ImplicitAcknowledgements),
                                                                                is_boolean(Force) ->

            {ok, Request} = subscribe(MasterUrl, FrameworkInfo, Force),                                                             
            {ok, #scheduler_state{
                                master_url = MasterUrl, 
                                implicit_ackowledgments = ImplicitAcknowledgements,
                                persistent_connection = Request,
                                handler_module = Module,
                                handler_state = State
            }};
        Else ->  
                Error = {bad_return_value, Else},   
                {stop, Error}                                           
    end.

handle_call(_, _, State) ->
    {reply, ok, State}.

handle_cast({teardown}, #scheduler_state{master_url = MasterUrl, framework_id = FrameworkId, persistent_connection = PersistantConnection } = State) ->

    Call = #'mesos.v1.scheduler.Call'{
        framework_id = FrameworkId,
        type = 'TEARDOWN'
    },

    ok = post(MasterUrl,Call),
    ok = httpc:cancel_request(PersistantConnection),

    {noreply, State};

handle_cast({accept, Message}, #scheduler_state{master_url = MasterUrl, framework_id = FrameworkId} = State) 

    when is_record(Message, 'mesos.v1.scheduler.Call.Accept') ->

    Call = #'mesos.v1.scheduler.Call'{
        framework_id = FrameworkId,
        accept = Message,
        type = 'ACCEPT'
    },

    ok = post(MasterUrl,Call),
    {noreply, State};

handle_cast({decline, Message}, #scheduler_state{master_url = MasterUrl, framework_id = FrameworkId} = State) 

    when is_record(Message, 'mesos.v1.scheduler.Call.Decline') ->

    Call = #'mesos.v1.scheduler.Call'{
        framework_id = FrameworkId,
        decline = Message,
        type = 'DECLINE'
    },

    ok = post(MasterUrl,Call),
    {noreply, State};

handle_cast({revive}, #scheduler_state{master_url = MasterUrl, framework_id = FrameworkId} = State) ->

    Call = #'mesos.v1.scheduler.Call'{
        framework_id = FrameworkId,
        type = 'REVIVE'
    },

    ok = post(MasterUrl,Call),
    {noreply, State};

handle_cast({kill, Message}, #scheduler_state{master_url = MasterUrl, framework_id = FrameworkId} = State) 

    when is_record(Message, 'mesos.v1.scheduler.Call.Kill') ->

    Call = #'mesos.v1.scheduler.Call'{
        framework_id = FrameworkId,
        kill = Message,
        type = 'KILL'
    },

    ok = post(MasterUrl,Call),
    {noreply, State};

handle_cast({shutdown, Message}, #scheduler_state{master_url = MasterUrl, framework_id = FrameworkId} = State) 

    when is_record(Message, 'mesos.v1.scheduler.Call.Shutdown') ->

    Call = #'mesos.v1.scheduler.Call'{
        framework_id = FrameworkId,
        shutdown = Message,
        type = 'SHUTDOWN'
    },

    ok = post(MasterUrl,Call),
    {noreply, State};

handle_cast({acknowledge, Message}, #scheduler_state{master_url = MasterUrl, framework_id = FrameworkId} = State) 
    when is_record(Message, 'mesos.v1.scheduler.Call.Acknowledge') ->

    Call = #'mesos.v1.scheduler.Call'{
        framework_id = FrameworkId,
        acknowledge = Message,
        type = 'ACKNOWLEDGE'
    },

    ok = post(MasterUrl,Call),
    {noreply, State};

handle_cast({reconcile, Message}, #scheduler_state{master_url = MasterUrl, framework_id = FrameworkId} = State) 
    when is_record(Message, 'mesos.v1.scheduler.Call.Reconcile') ->

    Call = #'mesos.v1.scheduler.Call'{
        framework_id = FrameworkId,
        reconcile = Message,
        type = 'RECONCILE'
    },

    ok = post(MasterUrl,Call),
    {noreply, State};

handle_cast({message, Message}, #scheduler_state{master_url = MasterUrl, framework_id = FrameworkId} = State) 
    when is_record(Message, 'mesos.v1.scheduler.Call.Message') ->

    Call = #'mesos.v1.scheduler.Call'{
        framework_id = FrameworkId,
        message = Message,
        type = 'MESSAGE'
    },

    ok = post(MasterUrl,Call),
    {noreply, State};

handle_cast({request, Message}, #scheduler_state{master_url = MasterUrl, framework_id = FrameworkId} = State) 
    when is_record(Message, 'mesos.v1.scheduler.Call.Request') ->

    Call = #'mesos.v1.scheduler.Call'{
        framework_id = FrameworkId,
        message = Message,
        type = 'REQUEST'
    },

    ok = post(MasterUrl,Call),
    {noreply, State}.

handle_info({http, {_, stream_start, _Headers}},State) ->
    % io:format("stream_start ~p~n", [Headers]), 
    {noreply, State};
handle_info({http, {_, stream_end, _Headers}},State) ->
    % io:format("stream_end ~p~n", [Headers]), 
    {noreply, State};
handle_info({http, {_, stream, BinBodyPart}}, State) ->
    BinaryBits = recordio:parse(BinBodyPart),
    Events = to_events(BinaryBits),
    State1 = lists:foldl(fun(Event, State1) -> State2 = dispatch_event(Event, State1), State2 end, State, Events),
    {noreply, State1}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

% callbacks
dispatch_event(Event, State) -> dispatch_event(Event#'mesos.v1.scheduler.Event'.type, Event, State).

dispatch_event('SUBSCRIBED', Event, #scheduler_state{ framework_id = undefined, handler_module = Module, handler_state = HandlerState } = State) ->

    #'mesos.v1.scheduler.Event.Subscribed'{ framework_id = FrameworkId } = Event#'mesos.v1.scheduler.Event'.subscribed,
    
    {ok, HandlerState1} = Module:subscribed( self(), HandlerState),
    State#scheduler_state{framework_id = FrameworkId, handler_state = HandlerState1};

dispatch_event('OFFERS', Event, #scheduler_state{ handler_module = Module, handler_state = HandlerState } = State) ->
    
    #'mesos.v1.scheduler.Event.Offers'{ offers = Offers} = Event#'mesos.v1.scheduler.Event'.offers,
    % TODO : look into why reverse offers are not in the protobuffer file
    {ok, HandlerState1} = Module:offers( self(), Offers, HandlerState),
    State#scheduler_state{handler_state = HandlerState1};

dispatch_event('RESCIND', Event, #scheduler_state{ handler_module = Module, handler_state = HandlerState } = State) ->
    
    #'mesos.v1.scheduler.Event.Rescind'{ offer_id = OfferId } = Event#'mesos.v1.scheduler.Event'.rescind,
    
    {ok, HandlerState1} = Module:rescind( self(), OfferId, HandlerState),
    State#scheduler_state{handler_state = HandlerState1};

dispatch_event('UPDATE', Event, #scheduler_state{ handler_module = Module, handler_state = HandlerState } = State) ->
    
    #'mesos.v1.scheduler.Event.Update'{ status = TaskStatus } = Event#'mesos.v1.scheduler.Event'.update,
    
    {ok, HandlerState1} = Module:update( self(), TaskStatus, HandlerState),
    State#scheduler_state{handler_state = HandlerState1};

dispatch_event('MESSAGE', Event, #scheduler_state{ handler_module = Module, handler_state = HandlerState } = State) ->
    
    #'mesos.v1.scheduler.Event.Message'{ agent_id = AgentId, executor_id = ExecutorId, data = Data } = Event#'mesos.v1.scheduler.Event'.update,
    
    {ok, HandlerState1} = Module:message( self(), AgentId, ExecutorId, Data, HandlerState),
    State#scheduler_state{handler_state = HandlerState1};
dispatch_event('FAILURE', Event, #scheduler_state{ handler_module = Module, handler_state = HandlerState } = State) ->
    
    #'mesos.v1.scheduler.Event.Failure'{ agent_id = AgentId, executor_id = ExecutorId, status = Status} = Event#'mesos.v1.scheduler.Event'.failure,
    
    {ok, HandlerState1} = Module:failure( self(), AgentId, ExecutorId, Status, HandlerState),
    State#scheduler_state{handler_state = HandlerState1};

dispatch_event('ERROR', Event, #scheduler_state{ handler_module = Module, handler_state = HandlerState } = State) ->
    
    #'mesos.v1.scheduler.Event.Error'{ message = Message} = Event#'mesos.v1.scheduler.Event'.error,

    {ok, HandlerState1} = Module:error( self(), Message, HandlerState),
    State#scheduler_state{handler_state = HandlerState1};

% TODO : implement reconnect logic with backoff based on this
dispatch_event('HEARTBEAT', Event, State) ->
    io:format("HEARTBEAT : ~p~n", [Event]), State.

% private 
to_events([]) -> [];
to_events([H|T]) ->
    Response = scheduler_pb:decode_msg(H, 'mesos.v1.scheduler.Event'),
    [Response | to_events(T)].

post(MasterUrl, Message) ->
    Method = post,
    URL = MasterUrl ++ ?SCHEDULER_API_URI,
    Header = [{"Accept", ?SCHEDULER_API_TRANSPORT}],
    Type = ?SCHEDULER_API_TRANSPORT,
    Body = scheduler_pb:encode_msg(Message),
    HTTPOptions = [],
    Options = [],
    {ok, _}= httpc:request(Method, {URL, Header, Type, Body}, HTTPOptions, Options),
    ok.

subscribe(MasterUrl, FrameworkInfo, Force) when is_list(MasterUrl), is_record(FrameworkInfo, 'mesos.v1.FrameworkInfo'), is_boolean(Force) ->
    
    Message = #'mesos.v1.scheduler.Call.Subscribe'{ 
        framework_info = FrameworkInfo, force = Force
    },

    Call = #'mesos.v1.scheduler.Call'{
        subscribe = Message,
        type = 'SUBSCRIBE'
    },

    Method = post,
    URL = MasterUrl ++ ?SCHEDULER_API_URI,
    Header = [{"Accept", ?SCHEDULER_API_TRANSPORT}],
    Type = ?SCHEDULER_API_TRANSPORT,
    Body = scheduler_pb:encode_msg(Call),
    HTTPOptions = [],
    Options = [{sync, false}, {stream, self}],
    % TODO : handle redirects and disconnections
    httpc:request(Method, {URL, Header, Type, Body}, HTTPOptions, Options).
