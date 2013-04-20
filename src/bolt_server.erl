%% Copyright
-module(bolt_server).
-author("pfeairheller@gmail.com").

-behavior(gen_server).

%% External API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

-record(state, {
  module,          % bolt handling module
  mod_state,
  event_man,
  run_loop_pid,
  output_pid
}).


start_link({Module, Args}, EventMgrRef) when is_atom(Module) ->
    ModState = apply(Module, prepare, [Args]),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Module, ModState, EventMgrRef], []).


init([Module, ModState, EventMgrRef]) ->
    { ok, #state{module = Module, mod_state = ModState, event_man = EventMgrRef} }.


terminate(_Reason, _State) ->
    ok.

handle_call({message, Tuple}, _From, #state{module=Module, mod_state=ModState } = State) ->
    apply(Module, execute, [Tuple, ModState]),
    {reply, ack, State};
handle_call({emit, Tuple}, _From,  #state{event_man = EventMgrRef } = State) ->
  gen_event:notify(EventMgrRef, Tuple),
  %% We'll have to get the modules that handled this event and pass them back to the Spout
  {reply, ack, State}.
    
handle_cast({message, _Tuple}, State) ->
    {noreply, State}.
