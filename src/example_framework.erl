%% -------------------------------------------------------------------
%% Copyright (c) 2015 Mark deVilliers.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------


-module (example_framework).
-behaviour (scheduler).

-include_lib("mesos_pb.hrl").

% api
-export ([exit/0]).

% from scheduler
-export ([init/1, 
          registered/3, 
          reregistered/2, 
          disconnected/1, 
          offerRescinded/2, 
          statusUpdate/2, 
          frameworkMessage/4, 
          slaveLost/2, 
          executorLost/4, 
          error/2,
          resourceOffers/2]).

-include_lib("mesos_pb.hrl").

-record (framework_state, { tasks_started = 0 }).

%
% Example framework (scheduler)
%
% Starts up, listens for resource offers, starts one task (the example executor), listens for updates
% When the task stops listens for more resource offers....
%
% scheduler:start_link( example_framework, "127.0.1.1:5050").

% api
init(MasterLocation) ->
    FrameworkInfo = #'FrameworkInfo'{user="", name="Erlang Test Framework"},
    State = #framework_state{},
    {FrameworkInfo, MasterLocation, State}.

exit() ->
    {ok,driver_stopped} = scheduler:stop(0), % stop the scheduler
    ok = scheduler:destroy(). % destroy and cleanup the nif

% call backs
registered(FrameworkID, MasterInfo, State) ->
    io:format("Registered callback : ~p ~p~n", [FrameworkID, MasterInfo]),
    {ok,State}.

reregistered(MasterInfo, State) ->
    io:format("ReRegistered callback : ~p ~n", [MasterInfo]),
    {ok,State}.

resourceOffers(Offer,#framework_state{ tasks_started = 1} = State) ->
    io:format("Reached max tasks [1] so declining offer.~n", []),
    scheduler:declineOffer(Offer#'Offer'.id),
    {ok,State};
resourceOffers(Offer, State) ->
    io:format("ResourceOffers callback : ~p ~n", [Offer]),
    State1 = State#framework_state{tasks_started = 1},
    io:format("Launching Task.", []),

    Scalar = mesos_pb:enum_symbol_by_value('Value.Type', 0),
    Resource1 = #'Resource'{name="cpus", type=Scalar, scalar=#'Value.Scalar'{value=1}},

    % example to launch a local executor - the example_executor.erl
    % will only work in development but proves the point
    {ok, CurrentFolder } = file:get_cwd(),

    Command = "export HOME=. && cd " ++ CurrentFolder ++" && erl -pa ebin -noshell -noinput -run example_executor main",

    {_,{H,M,S}} = calendar:local_time(),

    Id = integer_to_list(H) ++ integer_to_list(M) ++ integer_to_list(S),

    TaskInfo = #'TaskInfo'{
        name = "erlang_task" ++ Id,
        task_id = #'TaskID'{ value = "task_id_" ++ Id},
        slave_id = Offer#'Offer'.slave_id,
        resources = [Resource1],
        executor = #'ExecutorInfo'{ executor_id= #'ExecutorID'{ value = "executor_id_" ++ Id},
                                    command = #'CommandInfo'{value = Command}}
    },
    io:format("TaskInfo : ~p~n", [TaskInfo]),
    {ok,driver_running} = scheduler:launchTasks(Offer#'Offer'.id, [TaskInfo]),
    {ok,State1}.

disconnected(State) ->
    io:format("Disconnected callback"),
    {ok,State}.

offerRescinded(OfferID, State) ->
    io:format("OfferRescinded callback : ~p ~n", [OfferID]),
    {ok,State}.

statusUpdate( #'TaskStatus'{state='TASK_LOST',message=Reason}, State) ->
    io:format("StatusUpdate callback : ~p  -> task lost. Reason : ~p .~n", ['TASK_LOST', Reason]),
    {ok,State};
statusUpdate(#'TaskStatus'{state='TASK_RUNNING'}, State) ->
    io:format("StatusUpdate callback : ~p  -> task running.~n", ['TASK_RUNNING']),
    {ok,State};
statusUpdate( #'TaskStatus'{ state='TASK_FINISHED'}, State) ->
    io:format("StatusUpdate callback : ~p  -> decrementing current tasks.~n", ['TASK_FINISHED']),
    State1 = State#framework_state{tasks_started = 0},
    {ok,State1};
statusUpdate(StatusUpdate, State) ->
    io:format("StatusUpdate callback : ~p ~n", [StatusUpdate]),
    {ok,State}. 

frameworkMessage(ExecutorID, SlaveID, Message, State) ->
    io:format("FrameworkMessage callback : ~p ~p ~p ~n", [ExecutorID, SlaveID, Message]),
    {ok,State}.

slaveLost(SlaveID, State) ->
    io:format("SlaveLost callback : ~p ~n", [SlaveID]),
    {ok,State}.

executorLost(ExecutorID, SlaveID, Status, State) ->
    io:format("ExecutorLost callback : ~p ~p ~p ~n", [ExecutorID, SlaveID, Status]),
    {ok,State}.

error(Message, State) ->
    io:format("Error callback : ~p ~n", [Message]),
    {ok,State}.
