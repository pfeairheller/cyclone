%% Copyright
-module(spout).
-author("pfeairheller@gmail.com").

%% API
-export([behaviour_info/1]).


behaviour_info(callbacks) ->
  [{open, 3}, {next_tuple, 1}, {ack, 1}, {fail, 1}, {declare_output_fields, 1}];
behaviour_info(_) ->
  undefined.
