%% Copyright
-module(topology_sup).
-author("pfeairheller@gmail.com").

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-include("topology.hrl").
-include("spout_spec.hrl").
-include("bolt_spec.hrl").

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
  gen_event:start_link({local, topology_event_manager}),
  {ok, {{one_for_one, 5, 10}, build_child_specs(Topology)}}.
%% Somehow I have to add event handlers to the topology event manager to bind everything together.

build_child_specs(Topology) ->
  build_spouts(Topology#topology.spout_specs)
    ++ build_bolts(Topology#topology.bolts_specs)
    ++ build_groupings(Topology#topology.groupings).

build_spouts(SpoutSpecs) ->
  build_spouts(SpoutSpecs, []).

build_spouts([], ChildSpecs) ->
  ChildSpecs;
build_spouts([SpoutSpec | Rest], ChildSpecs) ->
  build_spouts(Rest, lists:flatten(build_spout(SpoutSpec), ChildSpecs)).

build_spout(SpoutSpec) ->
  build_spout(SpoutSpec#spout_spec.workers, SpoutSpec#spout_spec.id, SpoutSpec#spout_spec.spout, []).

build_spout(0, _SpoutId, _Spout, ChildSpecs) ->
  ChildSpecs;
build_spout(Workers, SpoutId, {Module, Args} = Spout, ChildSpecs) ->
  ChildId = SpoutId ++ integer_to_list(Workers),
  NewSpec = {ChildId, {spout_server, start_link, [list_to_atom(ChildId), {Module, Args}, topology_event_manager]}, permanent, brutal_kill, worker, [spout_server]},
  build_spout(Workers - 1, SpoutId, Spout, [NewSpec | ChildSpecs]).


build_bolts(BoltSpecs) ->
  build_bolts(BoltSpecs, []).

build_bolts([], ChildSpecs) ->
  ChildSpecs;
build_bolts([BoltSpec | Rest], ChildSpecs) ->
  build_bolts(Rest, lists:flatten(build_bolt(BoltSpec), ChildSpecs)).


build_bolt(BoltSpec) ->
  build_bolt(BoltSpec#bolt_spec.workers, BoltSpec#bolt_spec.id, BoltSpec#bolt_spec.bolt, []).

build_bolt(0, _BoltId, _Bolt, ChildSpecs) ->
  ChildSpecs;
build_bolt(Workers, BoltId, {Module, Args} = Bolt, ChildSpecs) ->
  ChildId = BoltId ++ integer_to_list(Workers),
  NewSpec = {ChildId, {bolt_server, start_link, [list_to_atom(ChildId), {Module, Args}, topology_event_manager]}, permanent, brutal_kill, worker, [bolt_server]},
  build_bolt(Workers - 1, BoltId, Bolt, [NewSpec | ChildSpecs]).


build_groupings(Groupings) ->
  build_groupings(Groupings, []).

build_groupings([], ChildSpecs) ->
  ChildSpecs;
build_groupings(Groupings, ChildSpecs) ->
  [].


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
          {bolt_server, start_link, [test_bolt, []]},
          permanent, brutal_kill, worker,
          [bolt_server]}],
        build_child_specs(#topology{name = "test", bolts_specs = [#bolt_spec{id = "test", bolt = {test_bolt, []}, workers = 1}]}))
    },
    {"Child spec for one spout.",
      ?_assertEqual([
        {"test1",
          {spout_server, start_link, [{test_spout, [1]}, topology_event_manager]},
          permanent, brutal_kill, worker,
          [spout_server]}],
        build_child_specs(#topology{name = "test", spout_specs = [#spout_spec{id = "test", spout = {test_spout, [1]}, workers = 1}]}))
    },
    {"Child spec for one bolt with 3 workers.",
      ?_assertEqual([
        {"test1",
          {bolt_server, start_link, [test_bolt, []]},
          permanent, brutal_kill, worker,
          [bolt_server]},
        {"test2",
          {bolt_server, start_link, [test_bolt, []]},
          permanent, brutal_kill, worker,
          [bolt_server]},
        {"test3",
          {bolt_server, start_link, [test_bolt, []]},
          permanent, brutal_kill, worker,
          [bolt_server]}],
        build_child_specs(#topology{name = "test", bolts_specs = [#bolt_spec{id = "test", bolt = {test_bolt, []}, workers = 3}]}))
    },
    {"Child spec for one bolt and one spout.",
      ?_assertEqual([
        {"spout_test1",
          {spout_server, start_link, [{test_spout, [1]}, topology_event_manager]},
          permanent, brutal_kill, worker,
          [spout_server]},
        {"bolt_test1",
          {bolt_server, start_link, [test_bolt, [2]]},
          permanent, brutal_kill, worker,
          [bolt_server]}
      ],
        build_child_specs(
          #topology{
            name = "test",
            bolts_specs = [#bolt_spec{id = "bolt_test", bolt = {test_bolt, [2]}, workers = 1}],
            spout_specs = [#spout_spec{id = "spout_test", spout = {test_spout, [1]}, workers = 1}]}))
    },
    {"Child spec for two bolts and three spouts.",
      ?_assertEqual([
        {"spout_test1",
          {spout_server, start_link, [{test_spout, [1]}, topology_event_manager]},
          permanent, brutal_kill, worker,
          [spout_server]},
        {"spout_test2",
          {spout_server, start_link, [{test_spout, [1]}, topology_event_manager]},
          permanent, brutal_kill, worker,
          [spout_server]},
        {"spout_test3",
          {spout_server, start_link, [{test_spout, [1]}, topology_event_manager]},
          permanent, brutal_kill, worker,
          [spout_server]},
        {"bolt_test1",
          {bolt_server, start_link, [test_bolt, [2]]},
          permanent, brutal_kill, worker,
          [bolt_server]},
        {"bolt_test2",
          {bolt_server, start_link, [test_bolt, [2]]},
          permanent, brutal_kill, worker,
          [bolt_server]}
      ],
        build_child_specs(
          #topology{
            name = "test",
            bolts_specs = [#bolt_spec{id = "bolt_test", bolt = {test_bolt, [2]}, workers = 2}],
            spout_specs = [#spout_spec{id = "spout_test", spout = {test_spout, [1]}, workers = 3}]}))
    }

  ].

-endif.


