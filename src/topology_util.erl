%% Copyright
-module(topology_util).
-author("pfeairheller").

-include("topology.hrl").

%% API
-export([generate_topology_graph/1]).

generate_topology_graph(Topology) ->
  Graph = digraph:new([acyclic]),
  SpoutVerticies = [digraph:add_vertex(Graph, Spout) || Spout <- Topology#topology.spout_specs],
  BoltVerticies = [digraph:add_vertex(Graph, Bolt) || Bolt <- Topology#topology.bolt_specs],
  Verticies = lists:foldl(fun(X, Map) -> dict:store(X#spout_spec.id, X, Map) end, dict:new(), SpoutVerticies),
  AllVerticies = lists:foldl(fun(X, Map) -> dict:store(X#bolt_spec.id, X, Map) end, Verticies, BoltVerticies),
  connect_graph(Graph, AllVerticies, BoltVerticies),
  {ok, Graph}.

connect_graph(_Graph, _Verticies, []) ->
  ok;
connect_graph(Graph, Verticies, [BoltVertex | Rest]) ->
  Groupings = BoltVertex#bolt_spec.groupings,
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
    {"Graph for single spout topology",
      fun() ->
        Topology = #topology{
          name = "test",
          bolt_specs = [#bolt_spec{id = "bolt_test", bolt = {test_bolt, [2]}, workers = 2, groupings = [#grouping{source = "spout_test"}, #grouping{source = "another_spout"}]}],
          spout_specs = [#spout_spec{id = "spout_test", spout = {test_spout, [1]}, workers = 3}, #spout_spec{id = "another_spout", spout = {test_spout, [1]}, workers = 3}]},
        {ok, Graph} = generate_topology_graph(Topology),

        Bolt = lists:nth(1, Topology#topology.bolt_specs),
        Spout = lists:nth(1, Topology#topology.spout_specs),

        ?_assertEqual(digraph:out_edges(Graph, Bolt), 2),
        ?_assertEqual(digraph:in_edges(Graph, Spout), 2)
      end
    }
  ].

-endif.

