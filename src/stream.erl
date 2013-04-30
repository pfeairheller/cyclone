%% Copyright
-module(stream).
-author("pfeairheller").

-behaviour(gen_fsm) .

-include("topology.hrl").
-include("spout_spec.hrl").
-include("bolt_spec.hrl").
-include("grouping.hrl").

%% API
-export([start_link/1]).
-export([emit/2, emit/3, ack/2]).
-export([init/1, waiting/2, transition/2]).


start_link({Topology, MsgId}) ->
  gen_fsm:start_link({local, Topology#topology.name ++ MsgId}, stream, Topology, []).

init(Topology) ->
  %% FIND AND LAUNCH THE FIRST BOLT
  CurrentBolt = 1,
  {ok, Graph} = generate_topology_graph(Topology),
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



generate_topology_graph(Topology) ->
  Graph = digraph:new([acyclic]),
  SpoutVerticies = [digraph:add_vertex(Graph, {Id, Spout}) || #spout_spec{id = Id, spout = Spout} <- Topology#topology.spout_specs],
  BoltVerticies = [digraph:add_vertex(Graph, {Id, Bolt, Groupings}) || #bolt_spec{id = Id, bolt= Bolt, groupings = Groupings} <- Topology#topology.bolts_specs],
  Verticies = lists:foldl(fun(X, Map) -> dict:store(element(1, X), X, Map) end, dict:new(), SpoutVerticies ++ BoltVerticies),
  connect_graph(Graph, Verticies, BoltVerticies),
  {ok, Graph}.

connect_graph(_Graph, _Verticies, []) ->
  ok;
connect_graph(Graph, Verticies, [BoltVertex | Rest]) ->
  Groupings = element(3, BoltVertex),
  connect_bolt(Graph, Verticies, BoltVertex, Groupings),
  connect_graph(Graph, Verticies, Rest).

connect_bolt(_Graph, _Verticies, _BoltVertex, []) ->
  ok;
connect_bolt(Graph, Verticies, BoltVertex, [Grouping | Rest]) ->
  case dict:find(Grouping#grouping.source, Verticies) of
    error ->
      ok;
    {ok, Source} ->
      digraph:add_edge(Graph, Source, BoltVertex, Grouping)
  end,
  connect_bolt(Graph, Verticies, BoltVertex, Rest).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

generate_topology_graph_test_() ->
  [
    {"Graph for topology",
      fun() ->
        {ok, Graph} = generate_topology_graph(#topology{
          name = "test",
          bolts_specs = [#bolt_spec{id = "bolt_test", bolt = {test_bolt, [2]}, workers = 2, groupings = [#grouping{source = "spout_test"}]}],
          spout_specs = [#spout_spec{id = "spout_test", spout = {test_spout, [1]}, workers = 3}]}),
        io:format("Here: ~p~n", [digraph_utils:topsort(Graph)]),
        ?assert(false)
      end

    }
  ].

-endif.
