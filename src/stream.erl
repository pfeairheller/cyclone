%% Copyright
-module(stream).
-author("pfeairheller").

-include("topology.hrl").
-include("spout_spec.hrl").
-include("bolt_spec.hrl").

%% API
-export([init/1, execute/2, execute/3]).


init(Topology) ->
  generate_topology_graph(Topology).

execute(_Topology, Tuple) ->
  io:format("Got tuple: ~p~n", [Tuple]),
  ok.

execute(_Topology, Tuple, _MessageId) ->
  io:format("Got tuple: ~p~n", [Tuple]),
  ok.

generate_topology_graph(Topology) ->
  Graph = digraph:new([acyclic]),
  SpoutVerticies = [digraph:add_vertex(Graph, {Id, Spout}) || #spout_spec{id = Id, spout = Spout} <- Topology#topology.spout_specs],
  BoltVerticies = [digraph:add_vertex(Graph, {Id, Bolt}) || #bolt_spec{id = Id, bolt= Bolt} <- Topology#topology.bolts_specs],
  connect_graph(Graph, SpoutVerticies, BoltVerticies, Topology#topology.groupings),
  {ok, Graph}.

connect_graph(Graph, SpoutVerticies, BoltVerticies, [Grouping | Rest]) ->



