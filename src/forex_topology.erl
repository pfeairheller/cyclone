%% Copyright
-module(forex_topology).
-author("pfeairheller").

-behaviour(topology).

%% API
-include("topology.hrl").

%% API
-export([init/0]).

init() ->
  #topology{
    spout_specs = [
      #spout_spec{id = "EURGBP", spout = {daily_tick_spout, "data/EURGBP_Ticks_08.04.2013-08.04.2013"}, reliable=false, workers=1}
    ],
    bolt_specs = [
    ]
  }.
