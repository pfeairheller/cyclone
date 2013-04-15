%% Copyright
-module(bolt).
-author("pfeairheller").

%% API
-export([behaviour_info/1]).


behaviour_info(callbacks) ->
  [{prepare, 3}, {execute, 1}, {cleanup, 0}, {declare_output_fields, 1}];
behaviour_info(_) ->
  undefined.
