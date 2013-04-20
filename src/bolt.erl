%% Copyright
-module(bolt).
-author("pfeairheller").

%% API
-export([behaviour_info/1]).

%% External API
-export([emit/2, emit/3]).


behaviour_info(callbacks) ->
  [{prepare, 3}, {execute, 2}, {cleanup, 0}, {declare_output_fields, 1}];
behaviour_info(_) ->
  undefined.


emit(Pid, Tuple) ->
  gen_server:cast(Pid, {emit, Tuple}).

emit(Pid, Tuple, MsgId) ->
  gen_server:call(Pid, {emit, Tuple, MsgId}).

