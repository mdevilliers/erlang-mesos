-module (scheduler).

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

-include_lib("include/mesos_pb.hrl").

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
    register(scheduler_loop, Pid),
    Result = nif_scheduler:init(Pid, FrameworkInfo, MasterLocation),
    Result.

start() ->
    nif_scheduler:start().
join() ->
    nif_scheduler:join().
abort() ->
    nif_scheduler:abort().
stop(Failover) ->
    nif_scheduler:stop(Failover).
declineOffer(OfferId)->
    nif_scheduler:declineOffer(OfferId).  
declineOffer(OfferId,Filter) ->
    nif_scheduler:declineOffer(OfferId,Filter).
killTask(TaskId)->
    nif_scheduler:killTask(TaskId).
reviveOffers()->
    nif_scheduler:reviveOffers().
sendFrameworkMessage(ExecuterId,SlaveId,Data) ->
    nif_scheduler:sendFrameworkMessage(ExecuterId,SlaveId,Data).
requestResources(Requests)->
    nif_scheduler:requestResources(Requests).
reconcileTasks(TaskStatus)->
    nif_scheduler:reconcileTasks(TaskStatus).
launchTasks(OfferId, TaskInfos)->
    nif_scheduler:launchTasks(OfferId, TaskInfos).
launchTasks(OfferId, TaskInfos, Filter)->
    nif_scheduler:launchTasks(OfferId, TaskInfos, Filter).
destroy() ->
    case nif_scheduler:destroy() of
        ok ->
            scheduler_loop ! {internal_shudown},
            ok;
        Other ->
            Other
    end.
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
        {internal_shudown} ->
                unregister(scheduler_loop),
                {shutdown_complete};        
        Any ->
            io:format("SCHEDULER: UNKNOWN MESSAGE : ~p~n", [Any]),
            loop(Module,State)
    after
        1000 ->
            loop(Module,State)
    end.

% helpers
int_to_ip(Ip)-> {Ip bsr 24, (Ip band 16711680) bsr 16, (Ip band 65280) bsr 8, Ip band 255}.
