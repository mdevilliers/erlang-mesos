-module (example_scheduler).

-export ([init/1, subscribed/2, inverse_offers/3, offers/3, rescind/3, update/3, message/5, failure/5, error/3]).

-behaviour (scheduler).
-include_lib("scheduler_pb.hrl").

-record (framework_state, { tasks_started = 0 }).

init(_) ->
    FrameworkInfo = #'mesos.v1.FrameworkInfo'{ user="mdevilliers", name="Erlang Test Framework"},
    MasterUrl = "http://localhost:5050",
    ImplicitAcknowledgements = true,
    Force = true,
    State = #framework_state{},
    { FrameworkInfo, MasterUrl, ImplicitAcknowledgements, Force, State }.

subscribed(Client, State) -> 
    io:format("subscribed! : ~p, ~p ~n", [Client, State]),
    {ok, State}.

inverse_offers(_Client, _Offers,State) -> {ok, State}.

offers(Client, Offers, State) -> 
    io:format("offers! : ~p, ~p, ~p ~n", [Client, Offers, State]),
    OfferIds = lists:foldr(fun (#'mesos.v1.Offer'{id = OfferId}, Acc) ->  [OfferId | Acc ] end, [], Offers),
    scheduler:decline(Client, OfferIds),
    {ok, State}.

rescind(_Client, _OfferId, State) -> {ok, State}.

update(_Client, _TaskStatus, State) -> {ok, State}.

message(_Client, _AgentId, _ExecutorId, _Data, State) -> {ok, State}.

failure(_Client, _AgentId, _ExecutorId, _Status, State) -> {ok, State}.

error(_Client, _Message, State) -> {ok, State}.