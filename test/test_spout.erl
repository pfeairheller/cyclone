%% Copyright
-module(test_spout).
-author("pfeairheller").

-behaviour(spout).

%% API
-export([open/1, next_tuple/2, declare_output_fields/1]).

open(_) ->
  {ok, undefined}.

next_tuple(_,_) ->
  {ok, undefined}.

declare_output_fields(_Args) ->
  {ok, ["words"]}.