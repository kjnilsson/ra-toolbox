-module(lock).
-behaviour(ra_machine).

%% @doc
%% simple lock machine implementation that ignores gnarly stuff like
%% noconnection. Still potentially usable in some scenarios such as when
%% nodes that are disconnected reset themselves _or_ when a fencing token
%% is in use

-export([
         %% ra_machine implementation
         init/1,
         apply/3,

         %% Client api
         acquire/2,
         release/2,

         %% Cluster management API
         start/2
        ]).

-record(lock, {holder :: undefined | pid(),
               waiting = queue:new() :: queue:queue({pid(), up | disconnected})
              }).

-type state() :: #{term() => #lock{}}.

-spec init(_) -> state().
init(_Config) -> #{}.

-type effect() :: {monitor, process, pid()} | [effect()].
-type meta() :: #{}.
-type cmd() :: {acquire | release, Key :: term(), pid()} |
               {down, pid(), Info :: term()}.

-spec apply(meta(), cmd(), state()) ->
    {state(), ok | not_acquired | {acquired, non_neg_integer()}} |
    {state(), ok | not_acquired | {acquired, non_neg_integer()}, effect()}.
apply(#{index := Idx}, {acquire, Key, Pid}, State) ->
    %% if noone holds the lock for this key, grant it and
    %% return {acquired, Idx} where the raft index can be used as a
    %% "fencing token" - else queue the pid and return `not_acquired`.
    %% If the lock later is acquired by the Pid it will send a message of the
    %% form `{acquired, Key, Idx}` to the Pid
    %% The Pid will also be monitored
    handle_aquire(Key, Pid, Idx, State);
apply(#{index := Idx}, {release, Key, Pid}, State0) ->
    %% releases the lock (if held) and grants the lock to the next waiting
    %% Pid
    release_lock(Key, Pid, Idx, State0);
apply(#{index := Idx}, {down, Pid, _Info}, State) ->
    %% if this pid holds any lock it releases them
    %% also removes this pid from the waiting queues
    %% NB: no specifici down reason handling
    handle_pid_down(Pid, Idx, State).

%% local

handle_aquire(Key, Pid, Idx, State) ->
    Effect = {monitor, process, Pid},
    case State of
        #{Key := #lock{holder = Holder,
                       waiting = Waiting} = Lock0}
          when is_pid(Holder) ->
            %% lock is held, queue the Pid
            Lock = Lock0#lock{waiting = queue:in({Pid, up}, Waiting)},
            %% monitor the pid
            {maps:put(Key, Lock, State), not_acquired, Effect};
        #{Key := #lock{holder = undefined,
                       waiting = []} = Lock0} ->
            Lock = Lock0#lock{holder = Pid},
            %% use the raft index as the fencing token
            {maps:put(Key, Lock, State), {acquired, Idx}, Effect};
         _ ->
            Lock = #lock{holder = Pid},
            {maps:put(Key, Lock, State), {acquired, Idx}, Effect}
    end.


handle_pid_down(Pid, Idx, State0) ->
    maps:fold(fun (Key, #lock{holder = H}, {S0, ok, Effs})
                    when Pid == H ->
                      {S, ok, Eff} = release_lock(Key, Pid, Idx, S0),
                      {S, ok, [Eff | Effs]};
                  (Key, #lock{waiting = W0} = L, {S0, ok, Effs}) ->
                  %% lock is not held just clean the waiting list here
                  W = [W || {P, _} = W <- W0, P =/= Pid],
                  {S0#{Key => L#lock{waiting = W}}, ok, Effs}
              end, {State0, ok, []}, State0).

release_lock(Key, Pid, Idx, State0) ->
    Effect = {demonitor, process, Pid},
    case State0 of
        #{Key := #lock{holder = Pid,
                       waiting = []}} ->
            %% the lock is held by the pid and noone is waiting.
            %% delete the lock key
            {maps:remove(Key, State0), ok, Effect};
        #{Key := #lock{holder = Pid,
                       waiting = Waiting0} = Lock0} ->
            %% If there are waiting processes dequeue the next up process
            %% and grant the lock to it
            case next_up(Waiting0, []) of
                {{NextPid, up}, Waiting} ->
                    %% no need to keep the lock key
                    Lock = Lock0#lock{waiting = Waiting},
                    Effects = [{send_msg, NextPid, {acquired, Key, Idx}}
                               | Effect],
                    %% monitor the pid
                    {maps:put(Key, Lock, State0), ok, Effects};
                {undefined, Waiting} ->
                    %% no suitable process available
                    Lock = Lock0#lock{holder = undefined,
                                      waiting = Waiting},
                    {maps:put(Key, Lock, State0), ok, Effect}
            end;
        _ ->
            {State0, ok}
    end.

next_up([{_, up} = Next | Rem], Disconnected) ->
    {Next, Disconnected ++ Rem};
next_up([], Disconnected) ->
    {undefined, Disconnected}.


%% Client api

acquire(ServerId, Key) ->
    ra:process_command(ServerId, {acquire, Key, self()}).

release(ServerId, Key) ->
    ra:process_command(ServerId, {release, Key, self()}).

%% Cluster api

start(Name, Nodes) when is_atom(Name) ->
    ServerIds = [{Name, N} || N <- Nodes],
    MachineConf = {module, ?MODULE, #{}},
    ra:start_cluster(Name, MachineConf, ServerIds).
