%% Copyright
-module(count_topology_mod).
-author("pfeairheller").

-behaviour(topology).

-include("topology.hrl").

%% API
-export([init/0]).

init() ->
  Spout = #spout_spec{id = "spout_1", spout = {test_spout, [1]}, workers = 3},
  Grouping = #grouping{type=field, source="spout_1"},
  Bolt1 = #bolt_spec{id = "bolt_1", bolt = {test_bolt, [2]}, workers = 2, stateful = true, groupings=[Grouping]},

  #topology{
    name = "test",
    bolt_specs = [Bolt1],
    spout_specs = [Spout]}.
