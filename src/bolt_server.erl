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
	  output_collector
	 }).


start_link({Module, Args}, OutputCollector) when is_atom(Module) ->
    ModState = apply(Module, prepare, [Args]),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Module, ModState, OutputCollector], []).


init([Module, ModState, OutputCollector]) ->
    { ok, #state{module = Module, mod_state = ModState, output_collector = OutputCollector} }.


terminate(_Reason, _State) ->
    ok.

handle_call({message, Tuple}, _From, #state{module=Module, mod_state=ModState, output_collector = OutputCollector} = State) ->
    {ok, ResultTuple} = apply(Module, prepare, [Tuple, ModState]),
    OutputCollector ! {message, ResultTuple},
    {reply, ack, State}.

handle_cast({message, _Tuple}, State) ->
    {noreply, State}.
