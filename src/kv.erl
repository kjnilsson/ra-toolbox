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


init(_Config) -> #{}.

apply(_Meta, {put, Key, Value}, State) ->
    {maps:put(Key, Value, State), inserted};
apply(_Meta, {delete, Key}, State0) ->
    State = maps:remove(Key, State0),
    {State, ok};
apply(_Meta, {get, Key}, State) ->
    Result = maps:get(Key, State, key_not_found),
    {State, Result}.


%% Client api

put(ServerId, Key, Value) ->
    ra:process_command(ServerId, {put, Key, Value}).

delete(ServerId, Key) ->
    ra:process_command(ServerId, {delete, Key}).

get(ServerId, Key) ->
    {ok, Res, _} = ra:consistent_query(ServerId,
                                       fun(State) ->
                                               maps:get(Key, State, key_not_found)
                                       end),
    Res.

%% Cluster api

start(Name, Nodes) when is_atom(Name) ->
    ServerIds = [{Name, N} || N <- Nodes],
    MachineConf = {module, ?MODULE, #{}},
    ra:start_cluster(Name, MachineConf, ServerIds).
