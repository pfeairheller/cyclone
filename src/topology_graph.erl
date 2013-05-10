%% Copyright
-module(topology_graph).
-author("pfeairheller").

-behaviour(gen_server).

-include("topology.hrl").

%% API
-export([start_link/1, register_spout_server/2, register_bolt_server/2, traverse/1, stop/0, get_spout_server/1]).

%% gen_server
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

%% API
start_link(Topology) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, Topology, []).

-record(traverse_state, {queue}).

traverse(Spout) when is_record(Spout, spout_spec) ->
  G = get_graph(),
  case digraph:vertex(G, Spout) of
    {Spout, _Label} ->
      Q = queue:from_list([Spout]),
      {ok, #traverse_state{queue=Q}, Spout};
    false ->
      done
  end;
traverse(#traverse_state{queue=Q}) ->
  G = get_graph(),
  case queue:out(Q) of
    {{value, Item}, Q2} ->
      Verticies = digraph:out_neighbours(G, Item),
      NewQ = queue:join(Q2, queue:from_list(Verticies)),
      case queue:peek(NewQ) of
        {value, Next} ->
          {ok, #traverse_state{queue=NewQ}, Next};
        empty ->
          done
      end;
    {empty, _Q} ->
      done
  end.

get_spout_server(Spout) ->
  G = get_graph(),
  case digraph:vertex(G, Spout) of
    {Spout, {Pid}} ->
      {ok, Pid};
    false ->
      {error, badspout}
   end.

register_spout_server(Spout, Pid) ->
  gen_server:call(?MODULE, {register_spout_server, Spout, Pid}).

register_bolt_server(Bolt, Pid) ->
  gen_server:call(?MODULE, {register_bolt_server, Bolt, Pid}).

stop() ->
  gen_server:call(?MODULE, stop).

%% gen_server callbacks
-record(state, {graph}).

init(Topology) ->
  {ok, Graph} = topology_util:generate_topology_graph(Topology),
  ets:new(?MODULE, [named_table]),
  ets:insert(?MODULE, {graph, Graph}),
  {ok, #state{graph = Graph}}.

handle_call({register_spout_server, Spout, Pid}, _From, #state{graph = Graph} = State) ->
  digraph:add_vertex(Graph, Spout, {Pid}),
  {reply, ok, State};
handle_call({register_bolt_server, Bolt, Pid}, _From, #state{graph = Graph} = State) ->
  digraph:add_vertex(Graph, Bolt, {Pid}),
  {reply, ok, State};
handle_call(stop, _From, State) ->
  ets:delete(?MODULE),
  {stop, normal, ok, State}.

handle_cast(_Request, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%Private

get_graph() ->
  [{graph, Graph}] = ets:lookup(?MODULE, graph),
  Graph.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

traverse_test_() ->
  [
    {"Traversing a moderately complex topology",
      fun() ->
        Spout = #spout_spec{id = "spout_1", spout = {test_spout, [1]}, workers = 3},
        Bolt1 = #bolt_spec{id = "bolt_1", bolt = {test_bolt, [2]}, workers = 2, stateful = true, groupings=[#grouping{type=field, source="spout_1"}]},
        Bolt2 = #bolt_spec{id = "bolt_2", bolt = {test_bolt, [2]}, workers = 2, stateful = true, groupings=[#grouping{type=field, source="spout_1"}]},
        Bolt3 = #bolt_spec{id = "bolt_3", bolt = {test_bolt, [2]}, workers = 2, stateful = true, groupings=[#grouping{type=field, source="spout_1"}]},
        Bolt4 = #bolt_spec{id = "bolt_4", bolt = {test_bolt, [2]}, workers = 2, stateful = true, groupings=[#grouping{type=field, source="bolt_1"}]},
        Bolt5 = #bolt_spec{id = "bolt_5", bolt = {test_bolt, [2]}, workers = 2, stateful = true, groupings=[#grouping{type=field, source="bolt_1"}]},
        Bolt6 = #bolt_spec{id = "bolt_6", bolt = {test_bolt, [2]}, workers = 2, stateful = true, groupings=[#grouping{type=field, source="bolt_2"}]},
        Bolt7 = #bolt_spec{id = "bolt_7", bolt = {test_bolt, [2]}, workers = 2, stateful = true, groupings=[#grouping{type=field, source="bolt_3"}]},
        Bolt8 = #bolt_spec{id = "bolt_8", bolt = {test_bolt, [2]}, workers = 2, stateful = true, groupings=[#grouping{type=field, source="bolt_3"}]},
        TraversalOrder = [Spout, Bolt3, Bolt2, Bolt1, Bolt8, Bolt7, Bolt6, Bolt5, Bolt4],

        T = #topology{
          name = "test",
          bolt_specs = [Bolt1, Bolt2, Bolt3, Bolt4, Bolt5, Bolt6, Bolt7, Bolt8],
          spout_specs = [Spout]},
        topology_graph:start_link(T),
        test_traversal(Spout, TraversalOrder),
        topology_graph:stop()
      end
    }
  ].

register_test_() ->
  [
    {"Register spout test",
      fun() ->
        Spout = #spout_spec{id = "spout_1", spout = {test_spout, [1]}, workers = 3},
        Bolt1 = #bolt_spec{id = "bolt_1", bolt = {test_bolt, [2]}, workers = 2, stateful = true, groupings=[#grouping{type=field, source="spout_1"}]},

        T = #topology{
          name = "test",
          bolt_specs = [Bolt1],
          spout_specs = [Spout]},
        topology_graph:start_link(T),
        topology_graph:register_spout_server(Spout, self()),

        {ok, Pid} = topology_graph:get_spout_server(Spout),
        ?assertEqual(Pid, self()),
        topology_graph:stop()
      end
    }
  ].

test_traversal(State, []) ->
  ?assertEqual(topology_graph:traverse(State), done);
test_traversal(State, [Expected | Order]) ->
  case topology_graph:traverse(State) of
    {ok, NewState, Node} ->
      io:format("~p~n:", [Node]),
      ?assertEqual(Expected, Node),
      test_traversal(NewState, Order);
    done ->
      ?assertEqual(length(Order), 0)
  end.

-endif.