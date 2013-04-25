%% Copyright
-module(field_grouping_handler).
-author("pfeairheller").

-behaviour(gen_event).

%% API
-export([init/1, handle_event/2, terminate/2, handle_call/2, handle_info/2, code_change/3]).

-record(field_map, {
   value,
   dest_pid
}).


init([Source, FieldNum, DestPids]) ->
  TableName = list_to_atom(string:to_lower(Source) ++ "_field_map"),
  ets:new(TableName, [named_table, {keypos, #field_map.value}]),
  {ok, {TableName, FieldNum, DestPids}}.

handle_event(Tuple, {TableName, FieldNum, DestPids} = State) ->
  Key = element(FieldNum, Tuple),
  case ets:lookup(TableName, Key) of
    [] ->
      [Pid | Rest] = DestPids,
      FieldMap = #field_map{value = Key, dest_pid = Pid},
      ets:insert(TableName, FieldMap),
      grouping_server:emit(Pid,  Tuple),
      {ok, {TableName, FieldNum, lists:append(Rest, [Pid])}};
    [#field_map{dest_pid = Pid}] ->
      grouping_server:emit(Pid, Tuple),
      {ok, State}
  end.

terminate(_Args, _State) ->
  ok.

handle_call(_Request, State) ->
  {ok, reply, State}.

handle_info(_Info, State) ->
  {ok, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
