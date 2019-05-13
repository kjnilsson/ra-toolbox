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
         state_enter/2,

         %% Client api
         acquire/2,
         release/2,

         %% Cluster management API
         start/2
        ]).

-type effect() :: {monitor, process, pid()} | [effect()].
-type meta() :: #{}.
-type cmd() :: {acquire | release, Key :: term(), pid()} |
               {down, pid(), Info :: term()}.

-record(lock, {holder :: undefined | pid(),
               token :: undefined | non_neg_integer(),
               waiting = [] :: [pid()]}).
-type state() :: #{term() => #lock{}}.

-spec init(_) -> state().
init(_Config) -> #{}.

-spec apply(meta(), cmd(), state()) ->
    {state(), ok | queued | {acquired, non_neg_integer()}} |
    {state(), ok | queued | {acquired, non_neg_integer()}, effect()}.
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
    release_lock(Key, Pid, Idx, State0, []);
apply(#{index := Idx}, {down, Pid, _Info}, State) ->
    %% if this pid holds any lock it releases them
    %% also removes this pid from the waiting queues
    %% NB: no specific down reason handling
    handle_pid_down(Pid, Idx, State).


state_enter(leader, State) ->
    %% re-request monitors for all known pids
    %% as well as notify all pids of the leader change so that they can monitor
    %% the new leader and end their session should it be disconnected
    Pids = all_pids(State),
    [{monitor, process, Pid} || Pid <- Pids] ++
    [{send_msg, leader_changed, Pid, ra_event} || Pid <- Pids];
state_enter(_, _) ->
    [].

%% internal

all_pids(State) ->
    lists:usort(
      [P || #lock{holder = P} <- maps:values(State), is_pid(P)]).

handle_aquire(Key, Pid, Idx, State) ->
    Effect = {monitor, process, Pid},
    case State of
        #{Key := #lock{holder = Pid,
                       token = Tok}} ->
            %% lock is already held just repeat the acquire
            {State, {acquired, Tok}, Effect};
        #{Key := #lock{holder = Holder,
                       waiting = Waiting} = Lock0}
          when is_pid(Holder) ->
            %% lock is held, queue the Pid
            Lock = Lock0#lock{waiting = Waiting ++ [Pid]},
            %% monitor the pid
            {maps:put(Key, Lock, State), queued, Effect};
        #{Key := #lock{holder = undefined,
                       waiting = []} = Lock0} ->
            Lock = Lock0#lock{holder = Pid,
                              token = Idx},
            %% use the raft index as the fencing token
            {maps:put(Key, Lock, State), {acquired, Idx}, Effect};
         _ ->
            Lock = #lock{holder = Pid,
                         token = Idx},
            {maps:put(Key, Lock, State), {acquired, Idx}, Effect}
    end.


handle_pid_down(Pid, Idx, State0) ->
    maps:fold(fun (Key, #lock{holder = H}, {S0, ok, Effs0})
                    when Pid == H ->
                      {S, ok, Effs} = release_lock(Key, Pid, Idx, S0, Effs0),
                      {S, ok, Effs};
                  (Key, #lock{waiting = W0} = L, {S0, ok, Effs}) ->
                  %% lock is not held just clean the waiting list here
                  W = [W || W <- W0, W =/= Pid],
                  {S0#{Key => L#lock{waiting = W}}, ok, Effs}
              end, {State0, ok, []}, State0).

release_lock(Key, Pid, Idx, State0, Effects0) ->
    Effect = {demonitor, process, Pid},
    case State0 of
        #{Key := #lock{holder = Pid,
                       waiting = []}} ->
            %% the lock is held by the pid and noone is waiting.
            %% delete the lock key
            {maps:remove(Key, State0), ok, [Effect | Effects0]};
        #{Key := #lock{holder = Pid,
                       waiting = Waiting0} = Lock0} ->
            %% If there are waiting processes dequeue the next up process
            %% and grant the lock to it
            case Waiting0 of
                [NextPid | Waiting] ->
                    %% no need to keep the lock key
                    Lock = Lock0#lock{waiting = Waiting},
                    Effects = [{send_msg, NextPid, {acquired, Key, Idx}},
                               Effect | Effects0],
                    %% monitor the pid
                    {maps:put(Key, Lock, State0), ok, Effects};
                [] ->
                    %% no suitable process available
                    Lock = Lock0#lock{holder = undefined,
                                      waiting = []},
                    {maps:put(Key, Lock, State0), ok, [Effect | Effects0]}
            end;
        _ ->
            {State0, ok, Effects0}
    end.

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
