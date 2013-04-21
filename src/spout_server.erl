%% Copyright
-module(spout_server).
-author("pfeairheller").

-behaviour(gen_server).

%% External API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, terminate/2, run_loop/1]).

-record(state, {
  module,          % spout handling module
  mod_state,
  event_man,
  run_loop_pid,
  output_pid
}).


start_link({Module, Args}, EventMgrRef) when is_atom(Module) ->
  ModState = apply(Module, open, [Args]),
  gen_server:start_link({local, ?MODULE}, ?MODULE, [Module, ModState, EventMgrRef], []).


init([Module, ModState, EventMgrRef]) ->
  State = #state{module = Module, mod_state = ModState, event_man = EventMgrRef},
  Pid = spawn_link(spout_server, run_loop, [State]),
  { ok, State#state{run_loop_pid = Pid, output_pid = self() } }.

terminate(_Reason, #state{run_loop_pid = Pid}) ->
  exit(Pid, kill).

handle_call({emit, Tuple}, _From, #state{event_man = EventMgrRef} = State) ->
  gen_event:notify(EventMgrRef, Tuple),
  %% We'll have to get the modules that handled this event and pass them back to the Spout
  {reply, ack, State};
handle_call({emit, Tuple, _MsgId}, _From, #state{event_man = EventMgrRef} = State) ->
  gen_event:notify(EventMgrRef, Tuple),
  %% GOTTA TRACK MESSAGES VIA THE MSGID HERE
  %% We'll have to get the modules that handled this event and pass them back to the Spout
  {reply, ack, State}.

handle_cast({emit, Tuple}, #state{event_man = EventMgrRef} = State) ->
  gen_event:notify(EventMgrRef, Tuple),
  {noreply, State};
handle_cast({emit, Tuple, _MsgId}, #state{event_man = EventMgrRef} = State) ->
  gen_event:notify(EventMgrRef, Tuple),
  {noreply, State}.


run_loop(#state{module = Module, mod_state = ModState, output_pid = Pid} = State ) ->
  apply(Module, next_tuple, [Pid, ModState]),
  run_loop(State).



-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").



-endif.
