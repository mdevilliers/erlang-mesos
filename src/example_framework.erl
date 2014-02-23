-module (example_framework).
-behaviour (gen_scheduler).

-include_lib("include/mesos.hrl").

% api
-export ([init/0]).

% from gen_scheduler
-export ([registered/2, 
          reregistered/1, 
          disconnected/0, 
          offerRescinded/1, 
          statusUpdate/1, 
          frameworkMessage/3, 
          slaveLost/1, 
          executorLost/3, 
          error/1,
          resourceOffers/1]).

% api
init()->
    FrameworkInfo = #'FrameworkInfo'{user="John Smith", name="Erlang Test Framework"},
    MasterLocation = "127.0.1.1:5050",
    ok = gen_scheduler:init(?MODULE, FrameworkInfo, MasterLocation),
    {ok,Status} = gen_scheduler:start(),
    Status.

% call backs
registered(FrameworkID,MasterInfo) ->
    io:format("Registered callback : ~p ~p~n", [FrameworkID, MasterInfo]).

reregistered(MasterInfo) ->
    io:format("ReRegistered callback : ~p ~n", [MasterInfo]).

disconnected() ->
    io:format("Disconnected callback").

offerRescinded(OfferID) ->
    io:format("OfferRescinded callback : ~p ~n", [OfferID]).

statusUpdate(StatusUpdate) ->
    io:format("StatusUpdate callback : ~p ~n", [StatusUpdate]). 

frameworkMessage(ExecutorID, SlaveID, Message) ->
    io:format("FrameworkMessage callback : ~p ~p ~p ~n", [ExecutorID, SlaveID, Message]).

slaveLost(SlaveID) ->
    io:format("SlaveLost callback : ~p ~n", [SlaveID]). 

executorLost(ExecutorID, SlaveID, Status) ->
    io:format("ExecutorLost callback : ~p ~p ~p ~n", [ExecutorID, SlaveID, Status]).

error(Message) ->
    io:format("Error callback : ~p ~n", [Message]).

resourceOffers(Offers) ->
    io:format("ResourceOffers callback : ~p ~n", [Offers]).