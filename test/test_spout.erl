%% Copyright
-module(test_spout).
-author("pfeairheller").

-behaviour(spout).

%% API
-export([open/1, next_tuple/2, declare_output_fields/1]).

open(_) ->
  {ok, undefined}.

next_tuple(Emitter,_) ->
  spout:emit(Emitter, "Test"),
  {ok, undefined}.

declare_output_fields(_Args) ->
  {ok, ["words"]}.