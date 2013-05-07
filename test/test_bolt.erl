%% Copyright
-module(test_bolt).
-author("pfeairheller").

-behaviour(bolt).

%% API
-export([prepare/1, execute/3]).

prepare(_) ->
  {ok, undefined}.

execute(Emitter,Tuple,_) ->
  bolt:emit(Emitter, Tuple),
  {ok, undefined}.
