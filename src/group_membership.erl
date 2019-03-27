-module(group_membership).
-behaviour(ra_machine).

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

-define(STATE, ?MODULE).

-type timestamp() :: integer().
-type status() :: up | {disconnected, When :: timestamp()}.
-type group() :: #{pid() => status()}.

-record(?STATE, {groups = #{} :: #{term() => group()}}).

-opaque state() :: #?STATE{}.
-export_type([state/0]).

init(_Config) -> #?STATE{}.

apply(_Meta, {join, GroupKey, Pid}, #?STATE{groups = Groups0} = State) ->
    Groups = maps:update_with(GroupKey,
                             fun(Group) -> Group#{Pid => up}  end,
                             #{Pid => up}, Groups0),
    Effect = {monitor, process, Pid},
    {State#?STATE{groups = Groups}, ok, Effect};
apply(_Meta, {leave, GroupKey, Pid}, #?STATE{groups = Groups0} = State) ->
    case maps:take(GroupKey, Groups0) of
        error ->
            {State, ok};
        {Group0, Groups} ->
            Group = maps:remove(Pid, Group0),
            Effect = {demonitor, process, Pid},
            {State#?STATE{groups = Groups#{GroupKey => Group}}, ok, Effect}
    end;
apply(#{system_time := Ts}, {down, Pid, noconnection},
      #?STATE{groups = Groups0} = State) ->
    %% This implementation assumes that erlang nodes will at come back at
    %% some point and thus processes on disconnected nodes may still be
    %% up.

    %% Mark the pid as disconnected and record the timestamp

    %% Update the pid in all groups
    Groups = maps:map(fun(_, Group) when is_map_key(Pid, Group) ->
                              Group#{Pid => {disconnected, Ts}};
                         (_, Group) ->
                              Group
                      end, Groups0),
    %% Monitor the node the pid is on (see {nodeup, Node} below)
    Effect = {monitor, node, node(Pid)},
    {State#?STATE{groups = Groups}, ok, Effect};
apply(_Meta, {down, Pid, _},
      #?STATE{groups = Groups0} = State) ->
    %% Any other down reason is either 'noproc' or an exit reason term so we can
    %% just remove the pid from any groups it belongs to
    Groups = maps:map(fun(_, Group) ->
                              maps:remove(Pid, Group)
                      end, Groups0),
    {State#?STATE{groups = Groups}, ok};
apply(_Meta, {nodeup, Node},
      #?STATE{groups = Groups0} = State) ->
    %% Now we need to work out if any pids that were disconnected are still
    %% alive.
    %% Re-request the monitor for the pids on this node
    Effects = [{monitor, process, Pid} || Pid <- disconnected_pids(Groups0),
                                          node(Pid) == Node],
    %% Update the status of these pids to 'up' as we will discover if they
    %% disappeared during the disconnection
    Groups = maps:map(fun(_, Group) ->
                              maps:map(fun (Pid, _) when node(Pid) == Node ->
                                               up;
                                           (_, Status) ->
                                               Status
                                       end, Group)
                      end, Groups0),
    {State#?STATE{groups = Groups}, ok, Effects};
apply(_Meta, {nodedown, _Node}, State) ->
    {State, ok}.

state_enter(leader, #?STATE{groups = Groups}) ->
    %% re-request monitors for all known pids
    Pids = maps:fold(fun(_, Group, Acc) ->
                             maps:fold(fun (Pid, _, A) ->
                                               A#{Pid => ''}
                                       end, Acc, Group)
                     end, #{}, Groups),
    io:format("state_enter: leader ~w", [Pids]),
    [{monitor, process, Pid} || Pid <- maps:keys(Pids)];
state_enter(_, _) ->
    [].


%% local functions

%% returns a unique list of disconnected Pids
disconnected_pids(Groups) ->
    Pids = maps:fold(fun(_, Group, Acc) ->
                               maps:fold(fun (Pid, {disconnected, _}, A) ->
                                                A#{Pid => ''};
                                            (_, _, A) ->
                                                A
                                        end, Acc, Group)
                       end, #{}, Groups),
    maps:keys(Pids).

%% Client API

join_group(ServerId, GroupKey, Pid) ->
    ra:process_command(ServerId, {join, GroupKey, Pid}).

leave_group(ServerId, GroupKey, Pid) ->
    ra:process_command(ServerId, {leave, GroupKey, Pid}).

query(ServerId, GroupKey) ->
    ra:leader_query(ServerId,
                    fun (#?STATE{groups = Groups}) ->
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
