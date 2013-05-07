%% Copyright
-module(topology_util).
-author("pfeairheller").

-include("topology.hrl").

%% API
-export([generate_topology_graph/1]).

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
    {"Graph for single spout topology",
      fun() ->
        {ok, Graph} = generate_topology_graph(#topology{
          name = "test",
          bolts_specs = [#bolt_spec{id = "bolt_test", bolt = {test_bolt, [2]}, workers = 2, groupings = [#grouping{source = "spout_test"}, #grouping{source = "another_spout"}]}],
          spout_specs = [#spout_spec{id = "spout_test", spout = {test_spout, [1]}, workers = 3}, #spout_spec{id = "another_spout", spout = {test_spout, [1]}, workers = 3}]}),
        io:format("Here: ~p~n", [digraph_utils:topsort(Graph)]),
        ?assert(false)
      end
    }
  ].

-endif.

