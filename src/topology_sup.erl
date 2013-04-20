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
  supervisor:start_link({local, ?MODULE}, ?MODULE, [Topogoly]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

%% Build the child spec out of the topology
init(Topology) ->
  {ok, { {one_for_one, 5, 10}, build_child_specs(Topology)} }.

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
  NewSpec = {ChildId, {spout_server, start_link, [Module, Args]}, permanent, brutal_kill, worker, [spout_server]},
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
  NewSpec = {ChildId, {bolt_server, start_link, [Module, Args]}, permanent, brutal_kill, worker, [bolt_server]},
  build_bolt(Workers - 1, BoltId, Bolt, [NewSpec | ChildSpecs]).


build_groupings(Groupings) ->
  build_groupings(Groupings, []).

build_groupings([], ChildSpecs) ->
  ChildSpecs;
build_groupings(Groupings, ChildSpecs) ->
  [].
