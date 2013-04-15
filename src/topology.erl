%% Copyright
-module(topology).
-author("pfeairheller").

%% API
-export([behaviour_info/1]).


behaviour_info(callbacks) ->
  [{init, 0}];
behaviour_info(_) ->
  undefined.
