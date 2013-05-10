%% Copyright
-module(topology_sup).
-author("pfeairheller@gmail.com").

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-include("topology.hrl").

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================


start_link(Module) ->
  Topogoly = apply(Module, init, []),
  supervisor:start_link({local, ?MODULE}, ?MODULE, Topogoly).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

%% Build the child spec out of the topology
init(Topology) ->
  TopologyGraphChild = {topology_graph, {topology_graph, start_link, [Topology]}, permanent, brutal_kill, worker, [topology_graph]},
  {ok, {{one_for_one, 5, 10}, [TopologyGraphChild | build_child_specs(Topology)]}}.

build_child_specs(Topology) ->
%% Find the spouts, and stateful bolts, lauch servers and add label's to the nodes with PIDs.
  build_spouts(Topology#topology.spout_specs)
    ++ build_bolts(Topology#topology.bolt_specs).

build_spouts(SpoutSpecs) ->
  build_spouts(SpoutSpecs, []).

build_spouts([], ChildSpecs) ->
  ChildSpecs;
build_spouts([SpoutSpec | Rest], ChildSpecs) ->
  build_spouts(Rest, lists:flatten(build_spout(SpoutSpec), ChildSpecs)).

build_spout(SpoutSpec) ->
  build_spout(SpoutSpec#spout_spec.workers, SpoutSpec#spout_spec.id, SpoutSpec, []).

build_spout(0, _SpoutId, _Spout, ChildSpecs) ->
  ChildSpecs;
build_spout(Workers, SpoutId, Spout, ChildSpecs) ->
  ChildId = SpoutId ++ integer_to_list(Workers),
  NewSpec = {ChildId, {spout_server, start_link, [list_to_atom(ChildId), Spout]}, permanent, brutal_kill, worker, [spout_server]},
  build_spout(Workers - 1, SpoutId, Spout, [NewSpec | ChildSpecs]).


build_bolts(BoltSpecs) ->
  build_bolts(BoltSpecs, []).

build_bolts([], ChildSpecs) ->
  ChildSpecs;
build_bolts([BoltSpec | Rest], ChildSpecs) ->
  build_bolts(Rest, lists:flatten(build_bolt(BoltSpec), ChildSpecs)).

build_bolt(BoltSpec) ->
  build_bolt(BoltSpec#bolt_spec.workers, BoltSpec#bolt_spec.id, BoltSpec#bolt_spec.bolt, BoltSpec#bolt_spec.stateful, []).

build_bolt(0, _BoltId, _Bolt, _Stateful, ChildSpecs) ->
  ChildSpecs;
build_bolt(_Workers, _BoltId, _Bolt, Stateful, ChildSpecs) when Stateful == false ->
  ChildSpecs;
build_bolt(Workers, BoltId, Bolt, Stateful, ChildSpecs) when Stateful == true ->
  ChildId = BoltId ++ integer_to_list(Workers),
  NewSpec = {ChildId, {bolt_server, start_link, [list_to_atom(ChildId), Bolt]}, permanent, brutal_kill, worker, [bolt_server]},
  build_bolt(Workers - 1, BoltId, Bolt, Stateful, [NewSpec | ChildSpecs]).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

start_link_test() ->
  {ok, _Pid} = topology_sup:start_link(test_topology_mod),
  ?assertNot(undefined == whereis(topology_sup)).

build_child_specs_test_() ->
  [
    {"Empty topology returns empty child specs",
      ?_assertEqual(build_child_specs(#topology{name = "test"}), [])},

    {"Child spec for one bolt.",
      ?_assertEqual([
        {"test1",
          {bolt_server, start_link, [test1, {test_bolt, []}]},
          permanent, brutal_kill, worker,
          [bolt_server]}],
        build_child_specs(#topology{name = "test", bolt_specs = [#bolt_spec{id = "test", bolt = {test_bolt, []}, workers = 1, stateful = true}]}))
    },
    {"Child spec for one spout.",
      fun() ->
        Spout1 = #spout_spec{id = "test", spout = {test_spout, [1]}, workers = 1},
        Expected = [{"test1", {spout_server, start_link, [test1, Spout1]}, permanent, brutal_kill, worker, [spout_server]}],
        Result = build_child_specs(#topology{name = "test", spout_specs = [Spout1]}),
        ?assertEqual(Expected, Result)
      end
    },
    {"Child spec for one bolt with 3 stateless workers.",
      fun() ->
        Expected = [],
        Result = build_child_specs(#topology{name = "test", bolt_specs = [#bolt_spec{id = "test", bolt = {test_bolt, []}, workers = 3}]}),
        ?assertEqual(Expected, Result)
      end
    },
    {"Child spec for one bolt with 3 stateful workers.",
      fun() ->
        Expected = [
          {"test1",
            {bolt_server, start_link, [test1, {test_bolt, []}]},
            permanent, brutal_kill, worker,
            [bolt_server]},
          {"test2",
            {bolt_server, start_link, [test2, {test_bolt, []}]},
            permanent, brutal_kill, worker,
            [bolt_server]},
          {"test3",
            {bolt_server, start_link, [test3, {test_bolt, []}]},
            permanent, brutal_kill, worker,
            [bolt_server]}],
        Result = build_child_specs(#topology{name = "test", bolt_specs = [#bolt_spec{id = "test", bolt = {test_bolt, []}, workers = 3, stateful = true}]}),
        ?assertEqual(Expected, Result)
      end
    },
    {"Child spec for one bolt and one spout.",
      fun() ->
        Spout1 = #spout_spec{id = "spout_test", spout = {test_spout, [1]}, workers = 1},
        Expected = [
          {"spout_test1",
            {spout_server, start_link, [spout_test1, Spout1]},
            permanent, brutal_kill, worker,
            [spout_server]},
          {"bolt_test1",
            {bolt_server, start_link, [bolt_test1, {test_bolt, [2]}]},
            permanent, brutal_kill, worker,
            [bolt_server]}
        ],
        Result = build_child_specs(
          #topology{
            name = "test",
            bolt_specs = [#bolt_spec{id = "bolt_test", bolt = {test_bolt, [2]}, workers = 1, stateful = true}],
            spout_specs = [Spout1]}),
        ?assertEqual(Expected, Result)
      end
    },
    {"Child spec for two bolts and three spouts.",
      fun() ->
        Spout1 = #spout_spec{id = "spout_test", spout = {test_spout, [1]}, workers = 3},
        Expected = [
          {"spout_test1",
            {spout_server, start_link, [spout_test1, Spout1]},
            permanent, brutal_kill, worker,
            [spout_server]},
          {"spout_test2",
            {spout_server, start_link, [spout_test2, Spout1]},
            permanent, brutal_kill, worker,
            [spout_server]},
          {"spout_test3",
            {spout_server, start_link, [spout_test3, Spout1]},
            permanent, brutal_kill, worker,
            [spout_server]},
          {"bolt_test1",
            {bolt_server, start_link, [bolt_test1, {test_bolt, [2]}]},
            permanent, brutal_kill, worker,
            [bolt_server]},
          {"bolt_test2",
            {bolt_server, start_link, [bolt_test2, {test_bolt, [2]}]},
            permanent, brutal_kill, worker,
            [bolt_server]}
        ],
        Result = build_child_specs(
          #topology{
            name = "test",
            bolt_specs = [#bolt_spec{id = "bolt_test", bolt = {test_bolt, [2]}, workers = 2, stateful = true}],
            spout_specs = [Spout1]}),
        ?assertEqual(Expected, Result)
      end
    }

  ].

-endif.


