-module(tasks).
-behaviour(ra_machine).

%% @doc
%% Simple task queue implementation that supports multiple workers
%% worker processes can "grab" an item from the
%% queue that is later completed. Should they fail before completing the
%% task the task will be enqueued at the front of the current queue
%% The original enqueuer can optionally be notified when the task has been
%% completed

-export([
         %% ra_machine implementation
         init/1,
         apply/3,
         state_enter/2,

         %% Client api
         enqueue/2,
         grab/1,
         complete/2,

         %% Cluster management API
         start/2
        ]).

-type task() :: {Id :: non_neg_integer(),
                 Enqueuer :: pid() | undefined,
                 TaskDetail :: term()}.

-record(state, {tasks = queue:new() :: queue:queue(task()),
                next_task_id = 1 :: non_neg_integer(),
                in_flight = #{} :: #{pid() => task()}
               }).

init(_Config) -> #state{}.

apply(_Meta, {enqueue, Pid, Detail}, #state{next_task_id = Next,
                                            tasks = Tasks0} =  State) ->
    Tasks = queue:in({Next, Pid, Detail}, Tasks0),
    %% no need to monitor enqueuer pids
    {State#state{tasks = Tasks,
                 next_task_id = Next + 1}, ok};
apply(_Meta, {grab, Pid}, #state{tasks = Tasks0,
                                 in_flight = InFlight} = State) ->
    case queue:out(Tasks0) of
        {empty, _} ->
            %% TODO: queue pid for asynchonous allocation once a task comes in?
            {State, empty};
        {{value, _}, _} when is_map_key(Pid, InFlight) ->
            %% the worker already has a task outstanding
            {State, {error, task_in_progress}};
        {{value, Task}, Tasks} ->
            Effect = {monitor, process, Pid},
            {State#state{tasks = Tasks,
                         in_flight = InFlight#{Pid => Task}},
             {ok, Task}, Effect}
    end;
apply(_Meta, {complete, Pid, TaskId}, #state{in_flight = InFlight0} = State) ->
    case maps:take(Pid, InFlight0) of
        {{TaskId, EnqueuerPid, Detail}, InFlight} ->
            Effects = [{send_msg, EnqueuerPid, {task_completed, Detail}},
                       {demonitor, process, Pid}],
            {State#state{in_flight = InFlight}, ok, Effects};
        _ ->
            %% must be a duplicate
            {State, ok}
    end;
apply(_Meta, {down, Pid, noconnection}, State) ->
    %% This implementation assumes that erlang nodes will at come back at
    %% some point and thus processes on disconnected nodes may still be
    %% Monitor the node the pid is on (see {nodeup, Node} below)
    %% so that we can detect when the node is re-connected and discover the
    %% actual fate of the process
    Effect = {monitor, node, node(Pid)},
    {State, ok, Effect};
apply(_Meta, {down, Pid, _Info}, #state{tasks = Tasks,
                                        in_flight = InFlight0} = State) ->
    case maps:take(Pid, InFlight0) of
        {Task, InFlight} ->
            %% return the in flight task to the front of the queue
            {State#state{tasks = queue:in_r(Task, Tasks),
                         in_flight = InFlight}, ok};
        _ ->
            %% must have missed to demonitor
            {State, ok}
    end;
apply(_Meta, {nodeup, Node}, State) ->
    %% Now we need to work out if any pids that were disconnected are still
    %% alive.
    %% Re-request the monitor for the pids on this node
    Effects = [{monitor, process, Pid} || Pid <- all_pids(State),
                                          node(Pid) == Node],
    {State, ok, Effects};
apply(_Meta, {nodedown, _Node}, State) ->
    {State, ok}.

state_enter(leader, State) ->
    %% re-request monitors for all known pids
    [{monitor, process, Pid} || Pid <- all_pids(State)];
state_enter(_, _) ->
    [].

%% internal

all_pids(#state{in_flight = InFlight}) ->
    maps:keys(InFlight).

%% Client api

enqueue(ServerId, Task) ->
    ra:process_command(ServerId, {enqueue, self(), Task}).

grab(ServerId) ->
    ra:process_command(ServerId, {grab, self()}).

complete(ServerId, TaskId) ->
    ra:process_command(ServerId, {complete, self(), TaskId}).

%% Cluster api

start(Name, Nodes) when is_atom(Name) ->
    ServerIds = [{Name, N} || N <- Nodes],
    MachineConf = {module, ?MODULE, #{}},
    ra:start_cluster(Name, MachineConf, ServerIds).
