%% Copyright
-module(shuffle_grouping_handler).
-author("pfeairheller").

-behaviour(gen_event).

%% gen_event
-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2,
  code_change/3]).

%% gen_event callbacks
-record(state, {source, dest_pids = []}).

init([Source, DestPids]) ->
  {ok, #state{source = Source, dest_pids = DestPids}}.

handle_event(Tuple, #state{dest_pids = DestPids} = State) ->
  Idx = random:uniform(length(DestPids) - 1),
  gen_server:emit(lists:nth(Idx, DestPids), Tuple),
  {ok, State}.

handle_call(_Request, State) ->
  {ok, reply, State}.

handle_info(_Info, State) ->
  {ok, State}.

terminate(_Arg, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
