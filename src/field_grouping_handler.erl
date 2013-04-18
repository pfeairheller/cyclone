%% Copyright
-module(field_grouping_handler).
-author("pfeairheller").

-behaviour(gen_event).

%% API
-export([init/1, handle_event/2, terminate/2]).

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
      Pid ! Tuple,
      {ok, {TableName, FieldNum, lists:append(Rest, [Pid])}};
    [#field_map{dest_pid = Pid}] ->
      Pid ! Tuple,
      {ok, State}
  end.

terminate(_Args, _State) ->
  ok.

