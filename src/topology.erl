%% Copyright
-module(topology).
-author("pfeairheller").

%% API
-export([behaviour_info/1, begin_traverse/2]).

%% Perhaps some pre/post stream processing callbacks...  maybe!!
behaviour_info(callbacks) ->
  [{init, 0}];
behaviour_info(_) ->
  undefined.


begin_traverse(Topology, Tuple) ->

  ok.


