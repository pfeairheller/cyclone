%% Copyright
-module(spout_server).
-author("pfeairheller").

-behaviour(gen_server).

%% External API
-export([start_link/2, run_loop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

-record(state, {
  module,          % bolt handling module
  mod_state,
  event_man
}).


start_link({Module, Args}, EventMgrRef) when is_atom(Module) ->
  ModState = apply(Module, open, [Args]),
  gen_server:start_link({local, ?MODULE}, ?MODULE, [Module, ModState, EventMgrRef], []).


init([Module, ModState, EventMgrRef]) ->
  State = #state{module = Module, mod_state = ModState, event_man = EventMgrRef},
  Pid = spawn_link(spout_server, run_loop, [State]),
  { ok, Pid }.


terminate(_Reason, _State) ->
  ok.

handle_call({message, Tuple}, _From, #state{module=Module, mod_state=ModState, event_man = EventMgrRef} = State) ->
  {ok, ResultTuple} = apply(Module, prepare, [Tuple, ModState]),
  gen_event:notify(EventMgrRef, ResultTuple),
  {reply, ack, State}.

handle_cast({message, _Tuple}, State) ->
  {noreply, State}.


run_loop(#state{module = Module, mod_state = ModState, event_man = EventMgrRef} ) ->
  %%CHECK MODULE FOR nextTuple, then recurse for the next message
  %%MAYBE MODULE USES THE PID FOR ~THIS~ GEN_SERVER TO SEND TUPLES TO, LIKE STORM'S OUTPUTCOLLECTOR!!!!!
  ok.