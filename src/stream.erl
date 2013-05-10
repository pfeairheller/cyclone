%% Copyright
-module(stream).
-author("pfeairheller").

-behaviour(gen_fsm) .

-include("topology.hrl").

%% API
-export([start_link/1]).
-export([emit/2, emit/3, ack/2]).
-export([init/1, waiting/2, transition/2]).


start_link({Spout, MsgId}) ->
  Graph = topology_graph:get_graph(),
  gen_fsm:start_link(stream, {Graph, Spout, MsgId}, []).

init({Graph, _Spout, _MsgId}) ->
  %% FIND AND LAUNCH THE FIRST BOLT
  CurrentBolt = 1,
  {ok, transition, {CurrentBolt, Graph}}.

emit(Pid, Tuple) ->
  gen_fsm:send_event(Pid, {message, Tuple}).

emit(Pid, Anchor, Tuple) ->
  gen_fsm:send_event(Pid, {message, Anchor, Tuple}).

ack(Pid, Anchor) ->
  gen_fsm:send_event(Pid, {ack, Anchor}).


waiting({message, Tuple}, Topology) ->
  %% Register the newly emitted tuple for further processing
  {next_state, waiting, Topology};
waiting({message, Anchor, Tuple}, Topology) ->
  %% Register the newly emitted tuple as a predecessor of Anchor
  {next_state, waiting, Topology};
waiting({ack, Anchor}, Topology) ->
  %% Register Anchor as finished with this bolt and move on to next bolt
  %% Find and LAUNCH the NEXT BOLT...
  {next_state, transition, Topology}.

transition({message, Tuple}, Topology) ->
  %% Register the newly emitted tuple for further processing
  {next_state, waiting, Topology};
transition({message, Anchor, Tuple}, Topology) ->
  %% Register the newly emitted tuple as a predecessor of Anchor
  {next_state, waiting, Topology};
transition({ack, Anchor}, Topology) ->
  %% Register Anchor as finished with this bolt and move on to next bolt
  %% Find and LAUNCH the NEXT BOLT...
  {next_state, transition, Topology}.



