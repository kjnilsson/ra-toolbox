-module(group_membership).
-behaviour(ra_machine).

%% simple group membership machine that automatically removes crashed processes
%% from the any groups but assumes that nodes eventually _will_ come back.
%% (no timeouts)

-export([
         init/1,
         apply/3,
         state_enter/2,

         %% Client api
         join_group/3,
         leave_group/3,
         query/2,

         %% Cluster management API
         start/2
         ]).

% -type key() :: term().
% -type group() :: #{pid() => ok}.
% -type state() :: #{key() => group()}.

init(_Config) -> #{}.

apply(_Meta, {join, GroupKey, Pid}, State0) ->
    State = maps:update_with(GroupKey,
                             fun(Group) -> Group#{Pid => ok}  end,
                             #{Pid => ok}, State0),
    %% we want to know if the process crashes so ask the Ra leader to monitor
    %% the process on our behalf and commit a {down, Pid, Info} entry to the log
    %% if a 'DOWN' is received
    Effect = {monitor, process, Pid},
    {State, ok, Effect};
apply(_Meta, {leave, GroupKey, Pid}, State0) ->
    case maps:take(GroupKey, State0) of
        error ->
            {State0, ok};
        {Group0, State} ->
            Group = maps:remove(Pid, Group0),
            %% TODO: check if the pid is in any other group
            %% if not then demonitor
            %% Effect = {demonitor, process, Pid},
            {State#{GroupKey => Group}, ok}
    end;
apply(_Meta, {down, Pid, noconnection}, State) ->
    %% This implementation assumes that erlang nodes will at come back at
    %% some point and thus processes on disconnected nodes may still be
    %% Monitor the node the pid is on (see {nodeup, Node} below)
    %% so that we can detect when the node is re-connected and discover the
    %% actual fate of the process
    Effect = {monitor, node, node(Pid)},
    {State, ok, Effect};
apply(_Meta, {down, Pid, _Info}, State0) ->
    %% Any other down reason is either 'noproc' or an exit reason term so we can
    %% just remove the pid from any groups it belongs to
    State = maps:map(fun(_, Group) ->
                             maps:remove(Pid, Group)
                     end, State0),
    {State, ok};
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


%% local functions

%% returns a unique list of known Pids
all_pids(Groups) ->
    Pids = maps:fold(fun(_, Group, Acc) ->
                               maps:merge(Group, Acc)
                     end, #{}, Groups),
    maps:keys(Pids).

%% Client API

join_group(ServerId, GroupKey, Pid) ->
    ra:process_command(ServerId, {join, GroupKey, Pid}).

leave_group(ServerId, GroupKey, Pid) ->
    ra:process_command(ServerId, {leave, GroupKey, Pid}).

query(ServerId, GroupKey) ->
    ra:leader_query(ServerId,
                    fun (Groups) ->
                            case Groups of
                                #{GroupKey := Members} ->
                                    maps:keys(Members);
                                _ ->
                                    []
                            end
                    end).

%% Cluster API

start(Name, Nodes) when is_atom(Name) ->
    application:ensure_all_started(ra),
    ServerIds = [{Name, N} || N <- Nodes],
    MachineConf = {module, ?MODULE, #{}},
    ra:start_cluster(Name, MachineConf, ServerIds).
