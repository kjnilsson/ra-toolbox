-module(kv).
-behaviour(ra_machine).

-export([
         %% ra_machine implementation
         init/1,
         apply/3,

         %% Client api
         put/3,
         delete/2,
         get/2,

         %% Cluster management API
         start/2
        ]).

%% ra_machine implementation

init(_Config) -> #{}.

apply(_Meta, {put, Key, Value}, State) ->
    {maps:put(Key, Value, State), ok};
apply(_Meta, {delete, Key}, State) ->
    {maps:remove(Key, State), ok}.

%% Client api

put(ServerId, Key, Value) ->
    {ok, Result, _Leader} = ra:process_command(ServerId, {put, Key, Value}),
    Result.

delete(ServerId, Key) ->
    {ok, Result, _Leader} = ra:process_command(ServerId, {delete, Key}),
    Result.

get(ServerId, Key) ->
    QueryFun = fun(State)
                     when is_map_key(Key, State) ->
                       {ok, maps:get(Key, State)};
                  (_State) ->
                       {error, key_not_found}
               end,
    {ok, Result, _} = ra:consistent_query(ServerId, QueryFun),
    Result.

%% Cluster api

start(Name, Nodes) when is_atom(Name) ->
    ServerIds = [{Name, N} || N <- Nodes],
    MachineConf = {module, ?MODULE, #{}},
    ra:start_cluster(Name, MachineConf, ServerIds).
