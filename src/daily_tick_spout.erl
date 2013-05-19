%% Copyright
-module(daily_tick_spout).
-author("pfeairheller").

-behaviour(spout).

%% API
-export([open/1, next_tuple/2, declare_output_fields/1]).


open(FileName) ->
  TickList = parse(FileName),
  %Load output fields from first line, loop on all other lines.
  {ok, TickList}.

next_tuple(Emitter, State) ->
  ok.

declare_output_fields(FileName) ->
  ["Time","Ask","Bid","AskVolume","BidVolume"].


parse(File) ->
  {ok, F} = file:open(File, [read, raw]),
  {ok, FirstLine} = file:read_line(F),
  parse(F, FirstLine, []).

parse(F, eof, Done) ->
  file:close(F),
  lists:reverse(Done);

parse(F, Line, Done) ->
  case file:read_line(F) of
    {ok, NextLine}  ->
      parse(F, NextLine, [parse_line(Line)|Done]);
    eof ->
      parse(F, eof, [parse_line(Line)|Done])
  end.




parse_line(Line) -> parse_line(Line, []).

parse_line([], Fields) -> lists:reverse(Fields);
parse_line("," ++ Line, Fields) -> parse_field(Line, Fields);
parse_line(Line, Fields) -> parse_field(Line, Fields).

parse_field("\"" ++ Line, Fields) -> parse_field_q(Line, [], Fields);
parse_field(Line, Fields) -> parse_field(Line, [], Fields).

parse_field("," ++ _ = Line, Buf, Fields) -> parse_line(Line, [lists:reverse(Buf)|Fields]);
parse_field([C|Line], Buf, Fields) -> parse_field(Line, [C|Buf], Fields);
parse_field([], Buf, Fields) -> parse_line([], [lists:reverse(Buf)|Fields]).

parse_field_q(Line, Fields) -> parse_field_q(Line, [], Fields).
parse_field_q("\"\"" ++ Line, Buf, Fields) -> parse_field_q(Line, [$"|Buf], Fields);
parse_field_q("\"" ++ Line, Buf, Fields) -> parse_line(Line, [lists:reverse(Buf)|Fields]);
parse_field_q([C|Line], Buf, Fields) -> parse_field_q(Line, [C|Buf], Fields).