-module (scheduler).

-behaviour(gen_server).

-include_lib("scheduler_pb.hrl").

-export([start/2, start_link/2, teardown/1, accept/3, accept/4,
         decline/2, decline/3, revive/1, kill/2, kill/3, shutdown/3,
         acknowledge/4, reconcile/2, message/4, request/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record (scheduler_state, {
        master_url,
        implicit_ackowledgments = false,
        persistent_connection,
        framework_id = undefined,
        handler_module,
        handler_state,
        stream_id
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
    -> {FrameworkInfo :: #'mesos.v1.FrameworkInfo'{}, MasterUrl :: string(),
        ImplicitAcknowledgements :: boolean(), Force :: boolean(), State :: any()}.

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
    hackney:start(),
    gen_server:start(?MODULE, {Module, Args}, []).

-spec start_link(Module :: atom(), Args :: term()) ->
    {ok, Server :: pid()} | {error, Reason :: term()}.

start_link(Module, Args ) ->
    hackney:start(),
    gen_server:start_link(?MODULE, {Module, Args}, []).

-spec teardown(Scheduler :: scheduler_client()) -> ok.

teardown(Scheduler) when is_pid(Scheduler) ->
    gen_server:cast(Scheduler, {teardown}).

-spec accept(Scheduler :: scheduler_client(),
             OfferIds :: offers(),
             Operations :: operations()) -> ok.

accept(Scheduler, OfferIds, Operations) when is_pid(Scheduler) ->

    gen_server:cast(Scheduler, {accept, #'mesos.v1.scheduler.Call.Accept'{
        offer_ids = OfferIds,
        operations = Operations
    }}).

-spec accept(Scheduler :: scheduler_client(),
             OfferIds :: offers(),
             Operations :: operations(),
             Filters :: #'mesos.v1.Filters'{}) -> ok.

accept(Scheduler, OfferIds, Operations, Filters) when is_pid(Scheduler),
                                                      is_record(Filters, 'mesos.v1.Filters') ->

    gen_server:cast(Scheduler, { accept, #'mesos.v1.scheduler.Call.Accept'{
        offer_ids = OfferIds,
        operations = Operations,
        filters = Filters
    }}).

-spec decline(Scheduler :: scheduler_client(),
             OfferIds :: offers()) -> ok.

decline(Scheduler, OfferIds) when is_pid(Scheduler)->

    gen_server:cast(Scheduler, { decline, #'mesos.v1.scheduler.Call.Decline'{
        offer_ids = OfferIds
    }}).

-spec decline(Scheduler :: scheduler_client(),
             OfferIds :: offers(),
             Filters :: #'mesos.v1.Filters'{}) -> ok.

decline(Scheduler, OfferIds, Filters) when is_pid(Scheduler),
                                           is_record(Filters, 'mesos.v1.Filters') ->

    gen_server:cast(Scheduler, {decline, #'mesos.v1.scheduler.Call.Decline'{
        offer_ids = OfferIds,
        filters = Filters
    }}).

-spec revive(Scheduler :: scheduler_client()) -> ok.

revive(Scheduler) when is_pid(Scheduler)  ->
    gen_server:cast(Scheduler, {revive}).

-spec kill (Scheduler :: scheduler_client(),
            TaskId :: #'mesos.v1.TaskID'{} ) -> ok.

kill(Scheduler, TaskId) when is_pid(Scheduler), is_record(TaskId, 'mesos.v1.TaskID') ->

    gen_server:cast(Scheduler, {kill, #'mesos.v1.scheduler.Call.Kill'{
            task_id = TaskId
    }}).

-spec kill (Scheduler :: scheduler_client(),
            TaskId :: #'mesos.v1.TaskID'{},
            AgentId :: #'mesos.v1.AgentID'{} ) -> ok.

kill(Scheduler, TaskId, AgentId) when is_pid(Scheduler),
                                      is_record(TaskId, 'mesos.v1.TaskID'),
                                      is_record(AgentId, 'mesos.v1.AgentID') ->

    gen_server:cast(Scheduler, {kill, #'mesos.v1.scheduler.Call.Kill'{
            task_id = TaskId,
            agent_id = AgentId
        }}).

-spec shutdown(Scheduler :: scheduler_client(),
               ExecutorId :: #'mesos.v1.ExecutorID'{},
               AgentId :: #'mesos.v1.AgentID'{}) -> ok.

shutdown(Scheduler, ExecutorId, AgentId) when is_pid(Scheduler),
                                              is_record(ExecutorId,'mesos.v1.ExecutorID'),
                                              is_record(AgentId,'mesos.v1.AgentID') ->

    gen_server:cast(Scheduler, {shutdown, #'mesos.v1.scheduler.Call.Shutdown'{
            executor_id = ExecutorId,
            agent_id = AgentId
        }}).

-spec acknowledge(Scheduler :: scheduler_client(),
               AgentId :: #'mesos.v1.AgentID'{},
               TaskId :: #'mesos.v1.TaskID' {},
               Uuid :: [byte()]) -> ok.

acknowledge(Scheduler, AgentId, TaskId, Uuid) when is_pid(Scheduler),
                                                   is_record(AgentId,'mesos.v1.AgentID'),
                                                   is_record(TaskId,'mesos.v1.TaskID') ->

    gen_server:cast(Scheduler, {acknowledge, #'mesos.v1.scheduler.Call.Acknowledge'{
        agent_id = AgentId, 
        task_id = TaskId,
        uuid = Uuid
    }}).

-spec reconcile(Scheduler :: scheduler_client(),
                Tasks :: reconcile_tasks()) -> ok.

reconcile(Scheduler, Tasks) when is_pid(Scheduler) ->

    gen_server:cast(Scheduler, {reconcile, #'mesos.v1.scheduler.Call.Reconcile'{
        tasks = Tasks
    }}).

-spec message(Scheduler :: scheduler_client(),
               AgentId :: #'mesos.v1.AgentID'{},
               ExecutorId :: #'mesos.v1.ExecutorID'{},
               Data :: [byte()]) -> ok.

message(Scheduler, AgentId, ExecutorId, Data) when is_pid(Scheduler),
                                                   is_record(AgentId, 'mesos.v1.AgentID'),
                                                   is_record(ExecutorId, 'mesos.v1.ExecutorID')->

    gen_server:cast(Scheduler, {message, #'mesos.v1.scheduler.Call.Message'{
        agent_id = AgentId,
        executor_id = ExecutorId,
        data = Data
    }}).

-spec request(Scheduler :: scheduler_client(),
               Requests :: requests()) -> ok.

request(Scheduler, Requests) when is_pid(Scheduler) -> 
    
    gen_server:cast(Scheduler, {request, #'mesos.v1.scheduler.Call.Request'{
        requests = Requests
    }}).

% gen server callbacks
init({Module, Args}) ->
    gen_server:cast(self(), {startup, Module, Args}),
    {ok, #scheduler_state{}}.

handle_call(_, _, State) ->
    {reply, ok, State}.

handle_cast({startup, Module, Args}, _)->

    case Module:init(Args) of
        { FrameworkInfo, MasterUrl, ImplicitAcknowledgements, Force, State }
                when is_record(FrameworkInfo, 'mesos.v1.FrameworkInfo'),
                     is_list(MasterUrl),
                     is_boolean(ImplicitAcknowledgements),
                     is_boolean(Force) ->

            {ok, Request} = subscribe(MasterUrl, FrameworkInfo, Force),
            State1 = #scheduler_state{
                                master_url = MasterUrl,
                                implicit_ackowledgments = ImplicitAcknowledgements,
                                persistent_connection = Request,
                                handler_module = Module,
                                handler_state = State},
            {noreply, State1};

        Else ->  
                Error = {bad_return_value, Else},
                {stop, Error, #scheduler_state{
                                handler_module = Module}
                                }
    end;

handle_cast({teardown}, #scheduler_state{ master_url = MasterUrl,
                                          framework_id = FrameworkId,
                                          persistent_connection = PersistantConnection,
                                          stream_id = StreamId
                                          } = State) ->
    ok = post(MasterUrl,#'mesos.v1.scheduler.Call'{
        framework_id = FrameworkId,
        type = 'TEARDOWN'
    },StreamId),
    hackney:close(PersistantConnection),

    {noreply, State};

handle_cast({accept, Message}, #scheduler_state{ master_url = MasterUrl,
                                                 framework_id = FrameworkId, stream_id = StreamId} = State)

    when is_record(Message, 'mesos.v1.scheduler.Call.Accept') ->

    ok = post(MasterUrl,#'mesos.v1.scheduler.Call'{
        framework_id = FrameworkId,
        accept = Message,
        type = 'ACCEPT'
    },StreamId),
    {noreply, State};

handle_cast({decline, Message}, #scheduler_state{ master_url = MasterUrl,
                                                  framework_id = FrameworkId, stream_id = StreamId} = State)

    when is_record(Message, 'mesos.v1.scheduler.Call.Decline') ->

    ok = post(MasterUrl,#'mesos.v1.scheduler.Call'{
        framework_id = FrameworkId,
        decline = Message,
        type = 'DECLINE'
    },StreamId),
    {noreply, State};

handle_cast({revive}, #scheduler_state{master_url = MasterUrl, framework_id = FrameworkId, stream_id = StreamId} = State) ->

    ok = post(MasterUrl,#'mesos.v1.scheduler.Call'{
        framework_id = FrameworkId,
        type = 'REVIVE'
    },StreamId),
    {noreply, State};

handle_cast({kill, Message}, #scheduler_state{master_url = MasterUrl, framework_id = FrameworkId, stream_id = StreamId} = State)
    when is_record(Message, 'mesos.v1.scheduler.Call.Kill') ->

    ok = post(MasterUrl,#'mesos.v1.scheduler.Call'{
        framework_id = FrameworkId,
        kill = Message,
        type = 'KILL'
    },StreamId),
    {noreply, State};

handle_cast({shutdown, Message}, #scheduler_state{master_url = MasterUrl, framework_id = FrameworkId, stream_id = StreamId} = State)
    when is_record(Message, 'mesos.v1.scheduler.Call.Shutdown') ->

    ok = post(MasterUrl, #'mesos.v1.scheduler.Call'{
        framework_id = FrameworkId,
        shutdown = Message,
        type = 'SHUTDOWN'
    },StreamId),
    {noreply, State};

handle_cast({acknowledge, Message}, #scheduler_state{master_url = MasterUrl, framework_id = FrameworkId, stream_id = StreamId} = State)
    when is_record(Message, 'mesos.v1.scheduler.Call.Acknowledge') ->

    ok = post(MasterUrl, #'mesos.v1.scheduler.Call'{
        framework_id = FrameworkId,
        acknowledge = Message,
        type = 'ACKNOWLEDGE'
    },StreamId),
    {noreply, State};

handle_cast({reconcile, Message}, #scheduler_state{master_url = MasterUrl, framework_id = FrameworkId, stream_id = StreamId} = State)
    when is_record(Message, 'mesos.v1.scheduler.Call.Reconcile') ->

    ok = post(MasterUrl, #'mesos.v1.scheduler.Call'{
        framework_id = FrameworkId,
        reconcile = Message,
        type = 'RECONCILE'
    },StreamId),
    {noreply, State};

handle_cast({message, Message}, #scheduler_state{master_url = MasterUrl, framework_id = FrameworkId, stream_id = StreamId} = State)
    when is_record(Message, 'mesos.v1.scheduler.Call.Message') ->

    ok = post(MasterUrl, #'mesos.v1.scheduler.Call'{
        framework_id = FrameworkId,
        message = Message,
        type = 'MESSAGE'
    },StreamId),

    {noreply, State};

handle_cast({request, Message}, #scheduler_state{master_url = MasterUrl, framework_id = FrameworkId, stream_id = StreamId} = State)
    when is_record(Message, 'mesos.v1.scheduler.Call.Request') ->

    ok = post(MasterUrl,#'mesos.v1.scheduler.Call'{
        framework_id = FrameworkId,
        message = Message,
        type = 'REQUEST'
    }, StreamId),
    {noreply, State}.

handle_info({hackney_response, _Ref, {status, _StatusInt, _Reason}}, State) ->
    %io:format("got ~p status: ~p with reason ~p~n", [_Ref, _StatusInt, _Reason]),
    {noreply,State};
handle_info({hackney_response, _Ref, {headers, Headers}}, State) ->
    %io:format(user, "hackney_response headers ~p ~p~n", [_Ref, Headers]),
    case hackney_headers:get_value(<<"Mesos-Stream-Id">>, hackney_headers:new(Headers), unknown) of
      unkown -> {stop,"'Mesos-Stream-Id' not found",State};
      Value ->  {noreply, State#scheduler_state{stream_id=Value}}
    end;

handle_info({hackney_response, _Ref, done}, State) ->
    %io:format(user, "hackney_response done ~p ~n", [_Ref]),
    {noreply,State};
handle_info( {hackney_response, _Ref, Bin}, State) ->
    % io:format(user, "hackney_response ~p ~p~n", [Ref, Bin]),
    BinaryBits = recordio:parse(Bin),
    Events = to_events(BinaryBits),
    State1 = lists:foldl(fun(Event, State1) -> State2 = dispatch_event(Event, State1), State2 end, State, Events),
    % io:format(user, "NEW STATE : ~p~n", [State1]),
    {noreply, State1}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

% private
dispatch_event(Event, State) -> dispatch_event(Event#'mesos.v1.scheduler.Event'.type, Event, State).

dispatch_event('SUBSCRIBED', Event, #scheduler_state{ framework_id = undefined, handler_module = Module, handler_state = HandlerState } = State) ->

    #'mesos.v1.scheduler.Event.Subscribed'{ framework_id = FrameworkId } = Event#'mesos.v1.scheduler.Event'.subscribed,

    {ok, HandlerState1} = Module:subscribed(self(), HandlerState),
    State#scheduler_state{framework_id = FrameworkId, handler_state = HandlerState1};

dispatch_event('OFFERS', Event, #scheduler_state{ handler_module = Module, handler_state = HandlerState } = State) ->

    #'mesos.v1.scheduler.Event.Offers'{ offers = Offers, inverse_offers = InverseOffers} = Event#'mesos.v1.scheduler.Event'.offers,

    {ok, HandlerState1} = offer_dispatch(Module, Offers, InverseOffers, HandlerState),
    State#scheduler_state{handler_state = HandlerState1};

dispatch_event('RESCIND', Event, #scheduler_state{ handler_module = Module, handler_state = HandlerState } = State) ->

    #'mesos.v1.scheduler.Event.Rescind'{ offer_id = OfferId } = Event#'mesos.v1.scheduler.Event'.rescind,

    {ok, HandlerState1} = Module:rescind(self(), OfferId, HandlerState),
    State#scheduler_state{handler_state = HandlerState1};

dispatch_event('UPDATE', Event, #scheduler_state{ handler_module = Module, handler_state = HandlerState, implicit_ackowledgments = ImplicitAcknowledgements } = State) ->

    #'mesos.v1.scheduler.Event.Update'{ status = TaskStatus } = Event#'mesos.v1.scheduler.Event'.update,

    acknowledgeStatusUpdate(ImplicitAcknowledgements, TaskStatus),

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

to_events([]) -> [];
to_events([H|T]) ->
    Response = scheduler_pb:decode_msg(H, 'mesos.v1.scheduler.Event'),
    [Response | to_events(T)].

acknowledgeStatusUpdate(false, _) -> ok;
acknowledgeStatusUpdate(true, #'mesos.v1.TaskStatus'{ uuid = <<>>}) -> ok ;
acknowledgeStatusUpdate(true, #'mesos.v1.TaskStatus'{task_id = TaskId,
                                                     agent_id = AgentId,
                                                     uuid = Uuid}) ->
    scheduler:acknowledge(self(), AgentId, TaskId, Uuid).

% only send out offers or inverse offers if they exist
% chain one after the other if they are received together
offer_dispatch(_, [], [], State) -> {ok, State};
offer_dispatch(Module, Offers, [], State) ->
    Module:offers(self(), Offers, State);
offer_dispatch(Module, [], InverseOffers, State) ->
    Module:inverse_offers(self(), InverseOffers, State);
offer_dispatch(Module, Offers, InverseOffers, State) ->
    {ok, State1} = Module:offers(self(), Offers, State),
    {ok, State2} = Module:inverse_offers(self(), InverseOffers, State1),
    {ok, State2}.

post(MasterUrl, Message, StreamId) ->

    URL = MasterUrl ++ ?SCHEDULER_API_URI,
    Headers = [{"Accept", ?SCHEDULER_API_TRANSPORT},
              {"Content-Type", ?SCHEDULER_API_TRANSPORT},
              {"Mesos-Stream-Id", StreamId}],

    Body = scheduler_pb:encode_msg(Message),

    case hackney:post(URL, Headers, Body) of
        {ok, 202, _, _} -> ok ;
        {ok, 400, _, Ref} ->
            % TODO : resolve this
            io:format("400 : ~p~n", [hackney:body(Ref)]),
            ok
    end.

subscribe(MasterUrl, FrameworkInfo, Force) when is_list(MasterUrl),
                            is_record(FrameworkInfo, 'mesos.v1.FrameworkInfo'),
                            is_boolean(Force) ->

    Message = #'mesos.v1.scheduler.Call.Subscribe'{
        framework_info = FrameworkInfo, force = Force
    },

    Call = #'mesos.v1.scheduler.Call'{
        subscribe = Message,
        type = 'SUBSCRIBE'
    },

    URL = MasterUrl ++ ?SCHEDULER_API_URI,
    Headers = [{"Accept", ?SCHEDULER_API_TRANSPORT},
               {"Content-Type", ?SCHEDULER_API_TRANSPORT}],
    Body = scheduler_pb:encode_msg(Call),
    Options = [async, {timeout, infinity}, {recv_timeout, infinity}],

    hackney:post(URL, Headers, Body, Options).
