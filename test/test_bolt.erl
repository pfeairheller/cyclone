%% Copyright
-module(test_bolt).
-author("pfeairheller").

-behaviour(bolt).

%% API
-export([prepare/1, execute/2]).

prepare(_) ->
  ok.

execute(_,_) ->
  ok.
