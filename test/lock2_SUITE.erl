-module(lock2_SUITE).

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
     acquire_release,
     async_acquire,
     async_acquire_disconnect
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
    W2 = erlang_node_helpers:start_erlang_node(w2, Config),
    ct:pal("Started Nodes ~w nodes(): ~w", [Nodes, nodes()]),
    PrivDir = ?config(priv_dir, Config),
    tcp_inet_proxy_helpers:enable_dist_proxy([W, W2 | Nodes], Config),
    [begin
         {ok, _} = ct_rpc:call(N, ra, start_in, [PrivDir]),
         ok = ct_rpc:call(N, ?MODULE, enable_logging, [PrivDir])
     end || N <- Nodes],

    [{nodes, Nodes}, {worker, W}, {worker2, W2} | Config].

end_per_suite(Config) ->
    erlang_node_helpers:stop_erlang_nodes(?config(nodes, Config)),
    erlang_node_helpers:stop_erlang_node(?config(worker, Config)),
    erlang_node_helpers:stop_erlang_node(?config(worker2, Config)),
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    {ok, Servers, []} = lock2:start(TestCase, ?config(nodes, Config)),
    [{servers, Servers} | Config].

end_per_testcase(TestCase, Config) ->
    Nodes = ?config(nodes, Config),
    ra:delete_cluster([{TestCase, N} || N <- Nodes]),
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

acquire_release(Config) ->
    Lock = ?FUNCTION_NAME,
    [N | _] = ?config(servers, Config),
    %% span
    {ok, acquired, Leader} = lock2:acquire(N, Lock),
    {ok, acquired, Leader} = lock2:acquire(N, Lock),
    {ok, ok, Leader} = lock2:release(N, Lock),
    {ok, acquired, Leader} = lock2:acquire(N, Lock),
    ok.

async_acquire(Config) ->
    Lock = ?FUNCTION_NAME,
    [N | _] = ?config(servers, Config),
    W = ?config(worker, Config),
    Self = self(),
    %% spawn lock holder
    Pid = spawn(W, fun () ->
                           {ok, acquired, _} = lock2:acquire(N, Lock),
                           Self ! try_acquire,
                           receive
                               release_please ->
                                   {ok, _, _} = lock2:release(N, Lock)
                           end
                   end),
    receive
        try_acquire -> ok
    after 2000 -> exit(try_acquire_timeout)
    end,
    {ok, queued, _} = lock2:acquire(N, Lock),
    Pid ! release_please,
    receive
        {acquired, Lock} ->
            ok
    after 5000 ->
              exit(async_acquire_timeout)
    end,
    ok.

async_acquire_disconnect(Config) ->
    Lock = ?FUNCTION_NAME,
    [N | _] = ?config(servers, Config),
    W = ?config(worker, Config),
    Self = self(),
    %% spawn lock holder
    _ = spawn(W, fun () ->
                         {ok, acquired, _} = lock2:acquire(N, Lock),
                         Self ! try_acquire,
                         receive
                             release_please ->
                                 {ok, _, _} = lock2:release(N, Lock)
                         end
                 end),
    receive
        try_acquire -> ok
    after 2000 -> exit(try_acquire_timeout)
    end,
    W2 = spawn(?config(worker2, Config),
               fun () ->
                       {ok, queued, {_, LeaderNode}} = lock2:acquire(N, Lock),
                       Self ! {w2_queued, LeaderNode},
                       receive
                           {acquired, Lock} ->
                               Self ! test_passed
                       after 80000 ->
                                 exit(async_acquire_timeout)
                       end
               end),
    monitor(process, W2),

    %% partition W from Leader node
    LeaderNode = receive
                     {w2_queued, L} ->
                         tcp_inet_proxy_helpers:block_traffic_between(W, L),
                         L
                 after 5000 ->
                           exit({w2_queued, timeout})
                 end,

    tcp_inet_proxy_helpers:allow_traffic_between(W, LeaderNode),

    receive
        test_passed ->
            ok
    after 80000 ->
              flush(),
              exit(test_failed)
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

flush() ->
    receive
        Any ->
            ct:pal("flush ~p", [Any]),
            flush()
    after 0 ->
              ok
    end.
