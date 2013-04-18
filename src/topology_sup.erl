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
  Topogoly = apply(Module, init, 1),
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
build_spouts(SpoutSpecs, ChildSpecs) ->
  [].

build_bolts(BoltSpecs) ->
  build_bolts(BoltSpecs, []).

build_bolts([], ChildSpecs) ->
  ChildSpecs;
build_bolts(BoltSpecs, ChildSpecs) ->
  [].

build_groupings(Groupings) ->
  build_groupings(Groupings, []).

build_groupings([], ChildSpecs) ->
  ChildSpecs;
build_groupings(Groupings, ChildSpecs) ->
  [].
