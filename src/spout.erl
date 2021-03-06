%% Copyright
-module(spout).
-author("pfeairheller@gmail.com").

%% behaviour
-export([behaviour_info/1]).

%% External API
-export([emit/2, emit/3]).

behaviour_info(callbacks) ->
  [{open, 1}, {next_tuple, 2}, {ack, 1}, {fail, 1}, {declare_output_fields, 1}];
behaviour_info(_) ->
  undefined.

emit(Pid, Tuple) ->
  spout_server:emit(Pid, Tuple).

emit(Pid, Tuple, MsgId) ->
  spout_server:emit(Pid, Tuple, MsgId).

