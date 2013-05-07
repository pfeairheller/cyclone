%% Copyright
-module(spout_server).
-author("pfeairheller").

-include("topology.hrl").

-behaviour(gen_server).

%% External API
-export([start_link/3, emit/2, emit/3, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, terminate/2, run_loop/1]).

-record(state, {
  spout_spec,          % spout handling module
  spout_state,
  graph,
  run_loop_pid,
  output_pid
}).


start_link(ChildId, Graph, SpoutId) ->
  case digraph:vertex(Graph, SpoutId) of
    {SpoutId, #spout_spec{spout={Module, Args}} = Spout} ->
      {ok, SpoutState} = apply(Module, open, [Args]),
      gen_server:start_link({local, ChildId}, ?MODULE, [Spout, SpoutState, Graph], []);
    false ->
      {error, bad_spout}
  end.


emit(Pid, Tuple) ->
  gen_server:call(Pid, {emit, Tuple}).

emit(Pid, Tuple, MsgId) ->
  gen_server:call(Pid, {emit, Tuple, MsgId}).
stop(Pid) ->
  gen_server:call(Pid, stop).

init([Spout, SpoutState, Graph]) ->
  State = #state{spout_spec = Spout, spout_state = SpoutState, graph = Graph, output_pid = self() },
  Pid = spawn_link(spout_server, run_loop, [State]),
  { ok, State#state{run_loop_pid = Pid} }.

terminate(_Reason, #state{run_loop_pid = Pid}) ->
  exit(Pid, kill).

handle_call({emit, Tuple}, _From, #state{spout_spec = Spout, graph = Graph} = State) ->
  {ok, StreamPid} = stream:start_link({Graph,Spout#spout_spec.id,undefined}),
  stream:emit(StreamPid, Tuple),
  {reply, ack, State};
handle_call({emit, Tuple, MsgId}, _From, #state{spout_spec = Spout, graph = Graph} = State) ->
  {ok, StreamPid} = stream:start_link({Graph,Spout#spout_spec.id,MsgId}),
  io:format("Started ~p~n", [StreamPid]),
  stream:emit(StreamPid, Tuple, MsgId),
  {reply, ack, State};
handle_call(stop, _From, State) ->
  {stop, normal, State}.

run_loop(#state{spout_spec = Spout, spout_state = SpoutState, output_pid = Pid}) ->
  {Module, _Args} = Spout#spout_spec.spout,
  {ok, NewSpoutState} = apply(Module, next_tuple, [Pid, SpoutState]),
  run_loop(#state{spout_spec = Spout, spout_state = NewSpoutState, output_pid = Pid}).



-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

start_link_test() ->
  SpoutSpec = #spout_spec{id= "test_spout", spout={test_spout, undefined}, workers=1},
  Graph = digraph:new(),
  digraph:add_vertex(Graph, "test_spout", SpoutSpec),
  {ok, _Pid} = spout_server:start_link(spout_test, Graph, "test_spout"),
  ?assertNot(undefined == whereis(spout_test)),

  receive
  after
  3000 ->
    ok
  end.



-endif.
