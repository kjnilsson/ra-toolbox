-module(tasks_SUITE).

-compile(export_all).

-export([
         ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     enqueue_grab_complete,
     crashing_worker
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_suite(Config0) ->
    inet_tcp_proxy_manager:start(),
    Config = [{erlang_dist_module, inet_proxy_dist} | Config0],
    Nodes = erlang_node_helpers:start_erlang_nodes([a, b, c], Config),
    W = erlang_node_helpers:start_erlang_node(w, Config),
    ct:pal("Started Nodes ~w nodes(): ~w", [Nodes, nodes()]),
    PrivDir = ?config(priv_dir, Config),
    tcp_inet_proxy_helpers:enable_dist_proxy([W | Nodes], Config),
    [begin
         {ok, _} = ct_rpc:call(N, ra, start_in, [PrivDir]),
         ok = ct_rpc:call(N, ?MODULE, enable_logging, [PrivDir])
     end || N <- Nodes],

    [{nodes, Nodes}, {worker, W} | Config].

end_per_suite(Config) ->
    erlang_node_helpers:stop_erlang_nodes(?config(nodes, Config)),
    erlang_node_helpers:stop_erlang_node(?config(worker, Config)),
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    {ok, Servers, []} = tasks:start(TestCase, ?config(nodes, Config)),
    [{servers, Servers} | Config].

end_per_testcase(TestCase, Config) ->
    Nodes = ?config(nodes, Config),
    ra:delete_cluster([{TestCase, N} || N <- Nodes]),
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

enqueue_grab_complete(Config) ->
    [N | _] = ?config(servers, Config),
    {ok, ok, Leader} = tasks:enqueue(N, task1),
    {ok, {ok, {TaskId, _EnqPid, task1}}, Leader} = tasks:grab(Leader),
    {ok, ok, Leader} = tasks:complete(Leader, TaskId),
    receive
        {task_completed, task1} ->
            ok
    after 2000 ->
              exit(task_completed_timeout)
    end,
    ok.

crashing_worker(Config) ->
    [N | _] = ?config(servers, Config),
    {ok, ok, Leader} = tasks:enqueue(N, task1),
    %% span a worker that never completes the task
    W = erlang:spawn(?config(worker, Config),
                     fun () ->
                             {ok, _, _} = tasks:grab(Leader),
                             exit(task_failure)
                     end),
    Ref = erlang:monitor(process, W),
    receive
        {'DOWN', Ref, _, _, task_failure} ->
            ok
    after 5000 ->
              exit(down_timeout)
    end,
    %% the process task youself
    {ok, {ok, {TaskId, _EnqPid, task1}}, Leader} = tasks:grab(Leader),
    {ok, ok, Leader} = tasks:complete(Leader, TaskId),
    receive
        {task_completed, task1} ->
            ok
    after 2000 ->
              exit(task_completed_timeout)
    end,
    ok.

%% Utility

enable_logging(DataDir) ->
    LogFile = filename:join([DataDir, atom_to_list(node()), "ra.log"]),
    SaslFile = filename:join([DataDir, atom_to_list(node()), "ra_sasl.log"]),
    logger:set_primary_config(level, debug),
    Config = #{config => #{type => {file, LogFile}}, level => debug},
    logger:add_handler(ra_handler, logger_std_h, Config),
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, {file, SaslFile}),
    application:stop(sasl),
    application:start(sasl),
    filelib:ensure_dir(LogFile),
    % _ = error_logger:logfile({open, LogFile}),
    _ = error_logger:tty(false),
    ok.
