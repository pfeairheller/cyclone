%% Copyright
-module(stream).
-author("pfeairheller").

%% API
-export([execute/2, execute/3]).

execute(_Topology, Tuple) ->
  io:format("Got tuple: ~p~n", [Tuple]),
  ok.

execute(_Topology, Tuple, _MessageId) ->
  io:format("Got tuple: ~p~n", [Tuple]),
  ok.
