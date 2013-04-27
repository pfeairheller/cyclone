%% Copyright
-module(spout_server).
-author("pfeairheller").

-behaviour(gen_server).

%% External API
-export([start_link/3, emit/2, emit/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, terminate/2, run_loop/1]).

-record(state, {
  module,          % spout handling module
  mod_state,
  topology,
  run_loop_pid,
  output_pid
}).


start_link(ChildId, {Module, Args}, Topology) when is_atom(Module) ->
  {ok, ModState} = apply(Module, open, [Args]),
  gen_server:start_link({local, ChildId}, ?MODULE, [Module, ModState, Topology], []).


emit(Pid, Tuple) ->
  gen_server:call(Pid, {emit, Tuple}).

emit(Pid, Tuple, MsgId) ->
  gen_server:call(Pid, {emit, Tuple, MsgId}).


init([Module, ModState, Topology]) ->
  State = #state{module = Module, mod_state = ModState, topology = Topology, output_pid = self() },
  Pid = spawn_link(spout_server, run_loop, [State]),
  { ok, State#state{run_loop_pid = Pid} }.

terminate(_Reason, #state{run_loop_pid = Pid}) ->
  exit(Pid, kill).

handle_call({emit, Tuple}, _From, #state{topology = Topology} = State) ->
  spawn_link(stream, execute, [Topology, Tuple]),
  %% We'll have to get the modules that handled this event and pass them back to the Spout
  {reply, ack, State};
handle_call({emit, Tuple, MsgId}, _From, #state{topology = Topology} = State) ->
  spawn_link(stream, execute, [Topology, Tuple, MsgId]),
  %% GOTTA TRACK MESSAGES VIA THE MSGID HERE
  %% We'll have to get the modules that handled this event and pass them back to the Spout
  {reply, ack, State}.

run_loop(#state{module = Module, mod_state = ModState, output_pid = Pid}) ->
  {ok, NewModState} = apply(Module, next_tuple, [Pid, ModState]),
  run_loop(#state{module = Module, mod_state = NewModState, output_pid = Pid}).



-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

emit_test() ->
  {ok, EvtMgrPid} = gen_event:start_link(),
  {ok, Pid} = spout_server:start_link({test_spout, undefined}, EvtMgrPid),

  spout_server:emit(Pid, {"test"}).

-endif.
