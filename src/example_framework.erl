%% -------------------------------------------------------------------
%% Copyright (c) 2014 Mark deVilliers.  All Rights Reserved.
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

-include_lib("include/mesos_pb.hrl").

% api
-export ([init/0, exit/0]).

% from scheduler
-export ([registered/3, 
          reregistered/2, 
          disconnected/1, 
          offerRescinded/2, 
          statusUpdate/2, 
          frameworkMessage/4, 
          slaveLost/2, 
          executorLost/4, 
          error/2,
          resourceOffers/2]).

-record (framework_state, { tasks_started = 0 }).

%
% Example framework (scheduler)
%
% Starts up, listens form resource offers, starts one task (example executor), listens for updates
% When the task stops listens from more resource offers....
%

% api
init()->

    FrameworkInfo = #'FrameworkInfo'{user="", name="Erlang Test Framework"},
    MasterLocation = "127.0.1.1:5050",
    State = #framework_state{},

    ok = scheduler:init(?MODULE, FrameworkInfo, MasterLocation, State),
    {ok,Status} = scheduler:start(),
    Status.

exit() ->
    {ok,driver_stopped} = scheduler:stop(0), % stop the scheduler
    ok = scheduler:destroy(). % destroy and cleanup the nif

% call backs
registered(State, FrameworkID, MasterInfo) ->
    io:format("Registered callback : ~p ~p~n", [FrameworkID, MasterInfo]),
    {ok,State}.

reregistered(State, MasterInfo) ->
    io:format("ReRegistered callback : ~p ~n", [MasterInfo]),
    {ok,State}.

resourceOffers(#framework_state{ tasks_started = 1} = State, Offer) ->
    io:format("Reached max tasks [1] so declining offer.~n", []),
    scheduler:declineOffer(Offer#'Offer'.id),
    {ok,State};
resourceOffers(State, Offer) ->
    io:format("ResourceOffers callback : ~p ~n", [Offer]),

    State1 = State#framework_state{tasks_started = 1},
    io:format("Launching Task.", []),

    Scalar = mesos_pb:enum_symbol_by_value('Value.Type', 0),
    Resource1 = #'Resource'{name="cpus", type=Scalar, scalar=#'Value.Scalar'{value=1}},

    % example to launch a local executor - the example_executor.erl
    % will only work in development but proves the point
    {ok, CurrentFolder } = file:get_cwd(),

    Command = "export HOME=/root && cd " ++ CurrentFolder ++" && erl -pa ebin -noshell -noinput -run example_executor init",

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

offerRescinded(State, OfferID) ->
    io:format("OfferRescinded callback : ~p ~n", [OfferID]),
    {ok,State}.

statusUpdate( State, {'TaskStatus',{'TaskID',_},'TASK_RUNNING',_,_,_,_}) ->
    io:format("StatusUpdate callback : ~p  -> task running current tasks.~n", ['TASK_RUNNING']),
    {ok,State};
statusUpdate( State, {'TaskStatus',{'TaskID',_},Message,_,_,_,_}) ->
    io:format("StatusUpdate callback : ~p  -> decrementing current tasks.~n", [Message]),
    State1 = State#framework_state{tasks_started = 0},
    {ok,State1};
statusUpdate(State, StatusUpdate) ->
    io:format("StatusUpdate callback : ~p ~n", [StatusUpdate]),
    {ok,State}. 

frameworkMessage(State, ExecutorID, SlaveID, Message) ->
    io:format("FrameworkMessage callback : ~p ~p ~p ~n", [ExecutorID, SlaveID, Message]),
    {ok,State}.

slaveLost(State, SlaveID) ->
    io:format("SlaveLost callback : ~p ~n", [SlaveID]),
    {ok,State}.

executorLost(State, ExecutorID, SlaveID, Status) ->
    io:format("ExecutorLost callback : ~p ~p ~p ~n", [ExecutorID, SlaveID, Status]),
    {ok,State}.

error(State, Message) ->
    io:format("Error callback : ~p ~n", [Message]),
    {ok,State}.
