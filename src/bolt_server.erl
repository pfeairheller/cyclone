%% Copyright
-module(bolt_server).
-author("pfeairheller@gmail.com").

-behavior(gen_server).

-include("topology.hrl").
%% External API
-export([start_link/1, tuple/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, terminate/2]).

-record(state, {
  module,    % bolt handling module
  mod_state
}).

start_link(#bolt_spec{bolt={Module, Args}} = Bolt) when is_atom(Module) ->
  {ok, ModState} = apply(Module, prepare, [Args]),
  gen_server:start_link(?MODULE, [Bolt, Module, ModState], []).


init([Bolt, Module, ModState]) ->
  topology_graph:register_bolt_server(Bolt, self()),
  {ok, #state{module = Module, mod_state = ModState}}.

tuple(Pid, Tuple) ->
  gen_server:call(Pid, {message, Tuple}).

terminate(_Reason, _State) ->
  ok.

handle_call({message, Tuple}, {Emitter, _Ref}, #state{module = Module, mod_state = ModState}) ->
  {ok, NewModState} = apply(Module, execute, [Emitter, Tuple, ModState]),
  {reply, ack, #state{module = Module, mod_state = NewModState}};
handle_call({message, Tuple, _MsgId}, Emitter, #state{module = Module, mod_state = ModState}) ->
  {ok, NewModState} = apply(Module, execute, [Emitter, Tuple, ModState]),
  {reply, ack, #state{module = Module, mod_state = NewModState}}.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

start_link_test() ->
  {ok, Pid} = bolt_server:start_link(test_bolt, {test_bolt, []}),
  ?assertNot(undefined == Pid),

  bolt_server:tuple(Pid, {test_tuple}),

  receive
    {'$gen_event',{message,{test_tuple}}} ->
      ok;
    Other ->
      io:format("Other: ~p~n", [Other]),
      ?assert(false)
  after
  1000 ->
    io:format("Timeout"),
    ?assert(false)
  end.


-endif.

