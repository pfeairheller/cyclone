%% Copyright
-module(bolt_server).
-author("pfeairheller@gmail.com").

-behavior(gen_server).

%% External API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

-record(state, {
  module, % bolt handling module
  mod_state,
  event_man,
  run_loop_pid,
  output_pid
}).

start_link({Module, Args}, EventMgrRef) when is_atom(Module) ->
  {ok, ModState} = apply(Module, prepare, [Args]),
  gen_server:start_link({local, ?MODULE}, ?MODULE, [Module, ModState, EventMgrRef], []).


init([Module, ModState, EventMgrRef]) ->
  {ok, #state{module = Module, mod_state = ModState, event_man = EventMgrRef, output_pid = self()}}.


terminate(_Reason, _State) ->
  ok.

handle_call({message, Tuple}, _From, #state{module = Module, mod_state = ModState, output_pid = Pid}) ->
  {ok, NewModState} = apply(Module, execute, [Pid, Tuple, ModState]),
  {reply, ack, #state{module = Module, mod_state = NewModState}};
handle_call({emit, Tuple}, _From, #state{event_man = EventMgrRef} = State) ->
  gen_event:notify(EventMgrRef, Tuple),
%% We'll have to get the modules that handled this event and pass them back to the Spout
  {reply, ack, State}.

handle_cast({message, Tuple}, #state{module = Module, mod_state = ModState, output_pid = Pid}) ->
  {ok, NewModState} = apply(Module, execute, [Pid, Tuple, ModState]),
  {noreply, #state{module = Module, mod_state = NewModState}}.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

start_link_test() ->
  {ok, _Pid} = bolt_server:start_link({test_bolt, []}, a),
  ?assertNot(undefined == whereis(bolt_server)).

-endif.

