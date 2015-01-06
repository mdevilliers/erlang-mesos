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

-module (example_executor).

-behaviour (executor).

-include_lib("mesos_pb.hrl").

% api
-export ([main/0, exit/0]).    

% from gen_executor
-export ([init/1,
          registered/4, 
          reregistered/2, 
          disconnected/1, 
          launchTask/2,  
          killTask/2, 
          frameworkMessage/2, 
          shutdown/1, 
          error/2]).

%
% Example executor
% 
% Starts up, sends a framework message, sends some task updates, sleeps for a while then closes.
%

main() ->
    executor:start_link( example_executor, []).

init(_State)->
    {ok, []}.

exit() ->
    {ok,driver_stopped} = executor:stop(), % stop the executor
    ok = executor:destroy(), % destroy and cleanup the nif
    io:format("Stopping! Bye...."),
    init:stop(). % exit the process

% call backs
registered(ExecutorInfo, FrameworkInfo, SlaveInfo, State) ->
    io:format("Registered callback : ~p ~p ~p~n", [ExecutorInfo, FrameworkInfo, SlaveInfo]),
    executor:sendFrameworkMessage("hello from the executor's registered callback"),
    {ok,State}.

reregistered(SlaveInfo, State) ->
    io:format("Reregistered callback : ~p ~n", [SlaveInfo]),
    {ok,State}.

disconnected(State) ->
    io:format("Disconnected callback~n", []),
    {ok,State}.

launchTask(TaskInfo, State) ->
    io:format("LaunchTask callback : ~p ~n", [TaskInfo]),

    TaskId = TaskInfo#'TaskInfo'.task_id,

    executor:sendStatusUpdate(#'TaskStatus'{task_id = TaskId , state='TASK_RUNNING'}),

    timer:sleep(5000), % do some work
    executor:sendStatusUpdate(#'TaskStatus'{task_id = TaskId , state='TASK_FINISHED'}),
    timer:sleep(50), % artifically slow the process down just to send the task_finished message 
    spawn(?MODULE, exit, []), % start closing down
    {ok,State}.

killTask(TaskID, State) ->
    io:format("KillTask callback : ~p ~n", [TaskID]),
    {ok,State}.

frameworkMessage(Message, State) ->
    io:format("FrameworkMessage callback : ~p ~n", [Message]),
    {ok,State}.

shutdown(State) ->
    io:format("Shutdown callback~n", []),
    {ok,State}.

error(Message, State) ->
    io:format("Error callback : ~p ~n", [Message]),
    {ok,State}.
