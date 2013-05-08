%% Copyright
-module(test_topology_mod).
-author("pfeairheller").

-behaviour(topology).

-include("topology.hrl").

%% API
-export([init/0]).

init() ->
  #topology{spout_specs = [], bolt_specs = []}.
