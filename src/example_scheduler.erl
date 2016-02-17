-module (example_scheduler).

-export ([init/1, subscribed/2, inverse_offers/3, offers/3, rescind/3, update/3, 
            message/5, failure/5, error/3]).

-behaviour (scheduler).
-include_lib("scheduler_pb.hrl").

-record (framework_state, { tasks_started = 0 }).

%
% Example framework (scheduler)
%
% Starts up, listens for resource offers, starts one task, listens for updates
% When the task stops listens for more resource offers....
%
% scheduler:start_link( example_scheduler, []).

init(_) ->
    
    CurrentUser = os:getenv("USER"),
    FrameworkInfo = #'mesos.v1.FrameworkInfo'{ user=CurrentUser, 
                                               name="Erlang Test Framework"},

    MasterUrl = "http://localhost:5050", 
    ImplicitAcknowledgements = true, 
    Force = true,

    { FrameworkInfo, MasterUrl, ImplicitAcknowledgements, Force, #framework_state{} }.

subscribed(_Client, State) -> 
    
    io:format("subscribed callback : ~p~n", [State]),
    {ok, State}.

inverse_offers(_Client, _Offers,State) -> 
    
    io:format("inverse offers callback : ~p~n", [State]),
    {ok, State}.

offers(Client, Offers, #framework_state{ tasks_started = 1} = State) ->
    
    io:format("Reached max tasks [1] so declining offer.~n", []),

    OfferIds = lists:foldr(
        fun (#'mesos.v1.Offer'{ id = OfferId}, Acc) -> 
            [OfferId | Acc ] end, [], Offers),

    scheduler:decline(Client, OfferIds),
    {ok,State}; 

offers(Client, [ #'mesos.v1.Offer'{ id = OfferId, agent_id = AgentId } | _] = Offers, State) -> 
    
    io:format("offers callback : ~p ~n", [Offers]),

    Id = get_unique_identifier(),

    Command = "env && sleep 10 && echo 'byebye'",

    Cpu = #'mesos.v1.Resource'{
            name="cpus", 
            type='SCALAR', 
            scalar=#'mesos.v1.Value.Scalar'{ 
                    value = 0.1 }
                },
    Memory = #'mesos.v1.Resource'{
            name="mem", 
            type='SCALAR', 
            scalar=#'mesos.v1.Value.Scalar'{ 
                    value = 32 }
                },


    TaskInfo = #'mesos.v1.TaskInfo'{
        name = "erlang_task_" ++ Id,
        task_id = #'mesos.v1.TaskID'{ value = "task_id_" ++ Id},
        agent_id = AgentId,
        resources = [ Cpu],
        executor = #'mesos.v1.ExecutorInfo'{ 
                    executor_id= #'mesos.v1.ExecutorID'{ 
                        value = "executor_id_" ++ Id
                        },
                    command = #'mesos.v1.CommandInfo'{
                        value = Command
                        },
                     resources = [ Cpu, Memory]
                    }
    },
   
    Operation = #'mesos.v1.Offer.Operation'{
        type = 'LAUNCH',
        launch = #'mesos.v1.Offer.Operation.Launch'{
            task_infos = [   
                TaskInfo
            ]
        } 
    },
    io:format("starting : ~p on ~p ~n", [Operation, AgentId]),

    ok = scheduler:accept(Client, [OfferId] , [Operation]),

    State1 = State#framework_state{tasks_started = 1},
    {ok, State1}.

rescind(_Client, OfferId, State) -> 
    
    io:format("rescind callback : OfferId : ~p ~n", [OfferId]), 
    {ok, State}.

update(_Client, #'mesos.v1.TaskStatus'{ state = 'TASK_LOST'} = TaskStatus, State) -> 
    
    io:format("update callback : TASKLOST : TaskStatus: ~p ~n", [TaskStatus]),
    State1 = State#framework_state{tasks_started = 0},
    {ok, State1};
update(_Client, TaskStatus, State) -> 
    io:format("update callback : TaskStatus: ~p ~n", [TaskStatus]), 
    {ok, State}.

message(_Client, AgentId, ExecutorId, Data, State) ->
    
    io:format("message callback : AgentId: ~p  ExecutorId :~p Data :~p~n", 
              [AgentId, ExecutorId, Data]), 
    {ok, State}.

failure(_Client, AgentId, ExecutorId, Status, State) ->
    
    io:format("failure callback : AgentId: ~p  ExecutorId :~p Status :~p~n", 
              [AgentId, ExecutorId, Status]), 
    {ok, State}.

error(_Client, Message, State) -> 
    
    io:format("message callback : Message: ~p~n", [Message]), 
    {ok, State}.

% private
get_unique_identifier() ->
    {_,{H,M,S}} = calendar:local_time(),
    integer_to_list(H) ++ integer_to_list(M) ++ integer_to_list(S).
