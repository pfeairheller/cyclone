%% Copyright
-module(spout_server).
-author("pfeairheller").

-include("topology.hrl").

-behaviour(gen_server).

%% External API
-export([start_link/1, emit/2, emit/3, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, terminate/2, run_loop/1]).

-record(state, {
  spout_spec,          % spout handling module
  spout_state,
  run_loop_pid,
  output_pid
}).


start_link(Spout) ->
  {Module, Args} = Spout#spout_spec.spout,
  {ok, SpoutState} = apply(Module, open, [Args]),
  gen_server:start_link(?MODULE, [Spout, SpoutState], []).

emit(Pid, Tuple) ->
  gen_server:call(Pid, {emit, Tuple}).

emit(Pid, Tuple, MsgId) ->
  gen_server:call(Pid, {emit, Tuple, MsgId}).
stop(Pid) ->
  gen_server:call(Pid, stop).

init([Spout, SpoutState]) ->
  topology_graph:register_spout_server(Spout, self()),
  State = #state{spout_spec = Spout, spout_state = SpoutState, output_pid = self() },
  Pid = spawn_link(spout_server, run_loop, [State]),
  { ok, State#state{run_loop_pid = Pid} }.

terminate(_Reason, #state{run_loop_pid = Pid}) ->
  exit(Pid, kill).

handle_call({emit, Tuple}, _From, #state{spout_spec = Spout} = State) ->
  {ok, StreamPid} = stream:start_link(Spout),
  stream:emit(StreamPid, Tuple),
  {reply, ack, State};
handle_call({emit, Tuple, MsgId}, _From, #state{spout_spec = Spout} = State) ->
  {ok, StreamPid} = stream:start_link({Spout#spout_spec.id,MsgId}),
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
  T = #topology{spout_specs=[SpoutSpec]},
  R = topology_graph:start_link(T),
  io:format("~p~n", [R]),
  {ok, Pid} = spout_server:start_link(SpoutSpec),
  ?assertNot(undefined == Pid),

  receive
  after 500 -> ok
  end.



-endif.
