%% Copyright
-module(stream).
-author("pfeairheller").

-behaviour(gen_server).

-include("topology.hrl").

%% API
-export([start_link/1, emit/2]).

%% gen_server
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

%% API
start_link(Spout) ->
  gen_server:start_link(?MODULE, Spout, []).

emit(StreamPid, Tuple) ->
  gen_server:call(StreamPid, {tuple, Tuple}).

%% gen_server callbacks
-record(state, {process_map}).

init(Spout) ->
  ProcessMap = dict:new(),
  {ok, #state{process_map = dict:store(self(), Spout, ProcessMap)}}.

handle_call({tuple, Tuple}, From, #state{process_map = ProcessMap} = State) ->
  case dict:find(From, ProcessMap) of
    {ok, Node} ->
      processTuple(Node, Tuple, ProcessMap);
    error ->
      error_logger:error_msg("Unknown source of Tuple: ~p.", [From]),
      {reply, ack, State}
  end.

handle_cast(_Request, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


%% private
processTuple(Node, Tuple, ProcessMap) ->
  Targets = topology_graph:outbound_bolts(Node),
  NewProcessMap = send_to_targets(Targets, Tuple, ProcessMap),
  {reply, ack, #state{process_map = NewProcessMap}}.


send_to_targets([], _Tuple, ProcessMap) ->
  ProcessMap;
send_to_targets([Target | Rest], Tuple, ProcessMap) ->
  {Grouping, Bolt} = Target,
  NewProcessMap = case send_to_target(Grouping, Bolt, Tuple) of
    {ok, stateless} ->
      ProcessMap;
    {ok, Pid} ->
      dict:store(Pid, Bolt, ProcessMap)
  end,
  send_to_targets(Rest, Tuple, NewProcessMap).

send_to_target(_Grouping, Bolt, Tuple) when Bolt#bolt_spec.stateful =:= false ->
  {Module, Args} = Bolt#bolt_spec.bolt,
  spawn_link(Module, execute, [self(),  Tuple, Args]);
send_to_target(_Grouping, Bolt, Tuple) when Bolt#bolt_spec.stateful =:= true ->
  case topology_graph:find_bolt_server(Bolt) of
    {ok, Pid} ->
      bolt:emit(Pid, Tuple);
    {error, unknown_bolt} ->
      ok
  end,
  {ok, stateless}.


