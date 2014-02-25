-module (gen_scheduler).

%api
-export([init/4,
        start/0,
        join/0,
        abort/0,
        stop/1,
        declineOffer/1,
        declineOffer/2,
        killTask/1,
        reviveOffers/0,
        sendFrameworkMessage/3,
        requestResources/1,
        reconcileTasks/1,
        launchTasks/2,
        launchTasks/3,
        destroy/0]).

-export([behaviour_info/1]).

% private
-export ([loop/2]).

-include_lib("include/mesos.hrl").

behaviour_info(callbacks) ->
    [ {registered, 3}, 
      {reregistered, 2}, 
      {disconnected, 1}, 
      {offerRescinded, 2}, 
      {statusUpdate, 2}, 
      {frameworkMessage, 4}, 
      {slaveLost, 2}, 
      {executorLost, 4} , 
      {error, 2}, 
      {resourceOffers, 2}];
behaviour_info(_Other) ->
    undefined.

init(Module, FrameworkInfo, MasterLocation, State) when is_record(FrameworkInfo, 'FrameworkInfo'), 
                                                 is_list(MasterLocation) ->
    Pid = spawn(?MODULE, loop, [Module, State]),
    Result = scheduler:init(Pid, FrameworkInfo, MasterLocation),
    Result.

start() ->
    scheduler:start().
join() ->
    scheduler:join().
abort() ->
    scheduler:abort().
stop(Failover) ->
    scheduler:stop(Failover).
declineOffer(OfferId)->
    scheduler:declineOffer(OfferId).  
declineOffer(OfferId,Filter) ->
    scheduler:declineOffer(OfferId,Filter).
killTask(TaskId)->
    scheduler:killTask(TaskId).
reviveOffers()->
    scheduler:reviveOffers().
sendFrameworkMessage(ExecuterId,SlaveId,Data) ->
    scheduler:sendFrameworkMessage(ExecuterId,SlaveId,Data).
requestResources(Requests)->
    scheduler:requestResources(Requests).
reconcileTasks(TaskStatus)->
    scheduler:reconcileTasks(TaskStatus).
launchTasks(OfferId, TaskInfos)->
    scheduler:launchTasks(OfferId, TaskInfos).
launchTasks(OfferId, TaskInfos, Filter)->
    scheduler:launchTasks(OfferId, TaskInfos, Filter).
destroy()->
    scheduler:destroy().

% main call back loop
loop(Module,State) -> 
    receive     
        {registered , FrameworkIdBin, MasterInfoBin } ->            
                FrameworkId = mesos_pb:decode_msg(FrameworkIdBin, 'FrameworkID'),
                MasterInfo = mesos_pb:decode_msg(MasterInfoBin, 'MasterInfo'),
                MasterInfo2 = MasterInfo#'MasterInfo'{ip = int_to_ip(MasterInfo#'MasterInfo'.ip)},
                {ok, State1} = Module:registered(State, FrameworkId, MasterInfo2),
                loop(Module, State1);
        {resourceOffers, OfferBin} ->
                Offer = mesos_pb:decode_msg(OfferBin, 'Offer'),
                 {ok, State1} = Module:resourceOffers(State,Offer),
                loop(Module, State1);
        {reregistered} ->
                {ok, State1} = Module:reregistered(State),
                loop(Module,State1);
        {disconnected} ->
                {ok, State1} = Module:disconnected(State),
                loop(Module, State1);   
        {offerRescinded, OfferIdBin} ->
                OfferId = mesos_pb:decode_msg(OfferIdBin, 'OfferID'),
                {ok, State1} = Module:offerRescinded(State,OfferId),
                loop(Module,State1);
        {statusUpdate, TaskStatusBin} ->
                TaskStatus = mesos_pb:decode_msg(TaskStatusBin, 'TaskStatus'),
                {ok, State1} = Module:statusUpdate(State,TaskStatus),
                loop(Module,State1);
        {frameworkMessage, ExecutorIdBin, SlaveIdBin, Message} ->
                ExecutorId = mesos_pb:decode_msg(ExecutorIdBin, 'ExecutorID'),
                SlaveId = mesos_pb:decode_msg(SlaveIdBin, 'SlaveID'),
                {ok, State1} = Module:frameworkMessage(State,ExecutorId,SlaveId,Message),
                loop(Module, State1);
        {slaveLost, SlaveIdBin} ->
                SlaveId = mesos_pb:decode_msg(SlaveIdBin, 'SlaveID'),
                {ok, State1} = Module:frameworkMessage(State,SlaveId),
                loop(Module,State1);
        {executorLost, ExecutorIdBin, SlaveIdBin, Status} ->
                ExecutorId = mesos_pb:decode_msg(ExecutorIdBin, 'ExecutorID'),
                SlaveId = mesos_pb:decode_msg(SlaveIdBin, 'SlaveID'),
                {ok, State1} = Module:executorLost(State, ExecutorId,SlaveId,Status),
                loop(Module,State1);
        {error, Message} ->
                {ok, State1} = Module:error(State,Message),
                loop(Module,State1);           
        Any ->
            io:format("other message from nif : ~p~n", [Any]),
            loop(Module,State)
    after
        1000 ->
            loop(Module,State)
    end.

% helpers
int_to_ip(Ip)-> {Ip bsr 24, (Ip band 16711680) bsr 16, (Ip band 65280) bsr 8, Ip band 255}.
