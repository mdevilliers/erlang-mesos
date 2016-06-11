-module (executor).

-behaviour(gen_server).

-include_lib("executor_pb.hrl").

-export([start/2, start_link/2, update/2, message/2]).

  % message Subscribe {
  %   repeated TaskInfo unacknowledged_tasks = 1;
  %   repeated Update unacknowledged_updates = 2;
  % }

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record (executor_state, {
        master_url,
        persistent_connection,
        handler_module,
        handler_state,
        environmentals,
        framework_id
        }).

-record (environmentals, {
        mesos_framework_id,
        mesos_executor_id,
        mesos_directory,
        mesos_agent_endpoint,
        mesos_checkpoint,
        mesos_recovery_timeout,
        mesos_subscription_backoff_max
        }).

-define (EXECUTOR_API_URI, "api/v1/executor").
-define (EXECUTOR_API_TRANSPORT, "application/x-protobuf").

% callback specifications
-type executor_client() :: pid().

-callback init( Args :: any())
    -> {State :: any()}.

-callback subscribed(Client :: executor_client(),
                     ExecutorInfo :: #'mesos.v1.ExecutorInfo'{},
                     AgentInfo ::  #'mesos.v1.AgentInfo'{},
                     State :: any())
    -> {ok, State :: any()}.

-callback launch(Client :: executor_client(),
                 TaskInfo :: #'mesos.v1.TaskInfo'{},
                 State :: any()) -> {ok, State :: any()}.

-callback kill(Client :: executor_client(),
               TaskId :: #'mesos.v1.TaskID'{},
               State :: any()) -> {ok, State :: any()}.

-callback acknowledged(Client :: executor_client(),
                       TaskId :: #'mesos.v1.TaskID'{},
                       Uuid :: list(), 
                       State :: any()) -> {ok, State :: any()}.

-callback message(Client :: executor_client(),
                  Data :: list(),
                  State :: any()) -> {ok, State :: any()}.

-callback shutdown(Client :: executor_client(),
                  GracePeriodInSeconds :: number(),
                  State :: any()) -> {ok, State :: any()}.

-callback error( Client :: executor_client(),
                  Message :: list(), 
                  State :: any()) -> {ok, State :: any()}.

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

-spec update(Executor :: executor_client(), 
             TaskStatus :: #'mesos.v1.TaskStatus'{}) -> ok.

update(Executor, TaskStatus) when is_pid(Executor),
                                  is_record(TaskStatus,'mesos.v1.TaskStatus') ->

    gen_server:cast(Executor, {update, #'mesos.v1.executor.Call.Update'{
        status = TaskStatus
    }}).

-spec message(Executor :: executor_client(),
             Data :: list()) -> ok.

message(Executor, Data) when is_pid(Executor),
                            is_list(Data) ->

    gen_server:cast(Executor, {message, #'mesos.v1.executor.Call.Message'{
        data = Data
    }}).

% gen server callbacks
init({Module, Args}) ->
    gen_server:cast(self(), {startup, Module, Args}),

    {ok, #executor_state{
        environmentals = load_environmental_variables()
    }}.

handle_call(_, _, State) ->
    {reply, ok, State}.

handle_cast({startup, Module, Args}, State)->

    case Module:init(Args) of
        { State} ->
            {ok, Request} = subscribe(),
            State1 = State#executor_state{
                                persistent_connection = Request,
                                handler_module = Module,
                                handler_state = State},
            {noreply, State1};

        Else ->
                Error = {bad_return_value, Else},
                {stop, Error, #executor_state{ handler_module = Module}}
    end;

handle_cast({message, Message}, State)
        when is_record(Message, 'mesos.v1.executor.Call.Message') ->

    ok = post(#'mesos.v1.executor.Call'{
        framework_id = nil,
        message = Message,
        type = 'MESSAGE'
    }),
    {noreply,State};

handle_cast({update, Update}, State)
        when is_record(Update, 'mesos.v1.executor.Call.Update') ->

    ok = post(#'mesos.v1.executor.Call'{
        framework_id = nil,
        update = Update,
        type = 'UPDATE'
    }),
    {noreply,State}.

handle_info({hackney_response, _Ref, {status, _StatusInt, _Reason}}, State) ->
    % io:format("got ~p status: ~p with reason ~p~n", [Ref, StatusInt, Reason]),
    {noreply,State};
handle_info({hackney_response, _Ref, {headers, _Headers}}, State) ->
    % io:format(user, "hackney_response headers ~p ~p~n", [Ref, Headers]),
    {noreply,State};
handle_info({hackney_response, _Ref, done}, State) ->
    % io:format(user, "hackney_response done ~p ~n", [Ref]),
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
to_events([]) -> [];
to_events([H|T]) ->
    Response = scheduler_pb:decode_msg(H, 'mesos.v1.executor.Event'),
    [Response | to_events(T)].

dispatch_event(Event, State) -> dispatch_event(Event#'mesos.v1.executor.Event'.type, Event, State).

dispatch_event('SUBSCRIBED', Event, #executor_state{ handler_module = Module, handler_state = HandlerState } = State) ->

    #'mesos.v1.executor.Event.Subscribed'{
        executor_info = nil,
        framework_info = nil,
        agent_info = AgentInfo } = Event#'mesos.v1.executor.Event'.subscribed,

    {ok, HandlerState1} = Module:subscribed(self(), AgentInfo, HandlerState),
    State#executor_state{framework_id = nil, handler_state = HandlerState1};

dispatch_event('LAUNCH', Event, #executor_state{ handler_module = Module, handler_state = HandlerState } = State) ->

    #'mesos.v1.executor.Event.Launch'{
        task = TaskInfo } = Event#'mesos.v1.executor.Event'.launch,

    {ok, HandlerState1} = Module:launch(self(), TaskInfo, HandlerState),
    State#executor_state{handler_state = HandlerState1};

dispatch_event('KILL', Event, #executor_state{ handler_module = Module, handler_state = HandlerState } = State) ->

    #'mesos.v1.executor.Event.Kill'{
        task_id = TaskID } = Event#'mesos.v1.executor.Event'.kill,

    {ok, HandlerState1} = Module:kill(self(), TaskID, HandlerState),
    State#executor_state{handler_state = HandlerState1};

dispatch_event('ACKNOWLEDGED', Event, #executor_state{ handler_module = Module, handler_state = HandlerState } = State) ->

    #'mesos.v1.executor.Event.Acknowledged'{
        task_id = TaskID, uuid = Uuid } = Event#'mesos.v1.executor.Event'.acknowledged,

    {ok, HandlerState1} = Module:acknowledged(self(), TaskID, Uuid, HandlerState),
    State#executor_state{handler_state = HandlerState1};

dispatch_event('MESSAGE', Event, #executor_state{ handler_module = Module, handler_state = HandlerState } = State) ->

    #'mesos.v1.executor.Event.Message'{
        data = Data } = Event#'mesos.v1.executor.Event'.message,

    {ok, HandlerState1} = Module:message(self(), Data, HandlerState),
    State#executor_state{handler_state = HandlerState1};


dispatch_event('SHUTDOWN', Event, #executor_state{ handler_module = Module, handler_state = HandlerState } = State) ->

    #'mesos.v1.executor.Event.Shutdown'{
        grace_period_seconds = GracePeriodInSeconds } = Event#'mesos.v1.executor.Event'.shutdown,

    {ok, HandlerState1} = Module:shutdown(self(), GracePeriodInSeconds, HandlerState),
    State#executor_state{handler_state = HandlerState1};

dispatch_event('ERROR', Event, #executor_state{ handler_module = Module, handler_state = HandlerState } = State) ->

    #'mesos.v1.executor.Event.Error'{
        message = Message } = Event#'mesos.v1.executor.Event'.message,

    {ok, HandlerState1} = Module:error(self(), Message, HandlerState),
    State#executor_state{handler_state = HandlerState1}.

post(Message) ->

    URL = "MasterUrl" ++ ?EXECUTOR_API_URI,
    Headers = [{"Accept", ?EXECUTOR_API_TRANSPORT},
              {"Content-Type", ?EXECUTOR_API_TRANSPORT}],

    Body = scheduler_pb:encode_msg(Message),

    case hackney:post(URL, Headers, Body) of
        {ok, 202, _, _} -> ok ;
        {ok, 400, _, Ref} ->
            % TODO : resolve this
            io:format("400 : ~p~n", [hackney:body(Ref)]),
            ok
    end.

load_environmental_variables()->
    #environmentals{
        mesos_framework_id = os:get_env("MESOS_FRAMEWORK_ID"),
        mesos_executor_id = os:get_env("MESOS_EXECUTOR_ID"),
        mesos_directory = os:get_env("MESOS_DIRECTORY"),
        mesos_agent_endpoint = os:get_env("MESOS_AGENT_ENDPOINT"),
        mesos_checkpoint = os:get_env("MESOS_CHECKPOINT"),
        mesos_recovery_timeout = os:get_env("MESOS_RECOVERY_TIMEOUT"),
        mesos_subscription_backoff_max = os:get_env("MESOS_SUBSCRIPTION_BACKOFF_MAX")
    }.
subscribe() -> {ok, nothing}.
% subscribe(MasterUrl, FrameworkInfo, Force) when is_list(MasterUrl), 
%                             is_record(FrameworkInfo, 'mesos.v1.FrameworkInfo'), 
%                             is_boolean(Force) ->
    
%     Message = #'mesos.v1.scheduler.Call.Subscribe'{ 
%         framework_info = FrameworkInfo, force = Force
%     },

%     Call = #'mesos.v1.scheduler.Call'{
%         subscribe = Message,
%         type = 'SUBSCRIBE'
%     },

%     URL = MasterUrl ++ ?SCHEDULER_API_URI,
%     Headers = [{"Accept", ?SCHEDULER_API_TRANSPORT},
%                {"Content-Type", ?SCHEDULER_API_TRANSPORT}],
%     Body = scheduler_pb:encode_msg(Call),
%     Options = [async, {timeout, infinity}, {recv_timeout, infinity}],

%     hackney:post(URL, Headers, Body, Options).
