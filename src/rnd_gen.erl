%%%-------------------------------------------------------------------
%%% @author maciek
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. mar 2016 15:34
%%%-------------------------------------------------------------------
-module(rnd_gen).
-author("maciek").

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).
-define(RAND_ALGO, exs64).
-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================
-export([rnd_expotential/1,rnd_erlang/2,rnd_uniform/2]).
%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).
rnd_expotential(Alpha) ->
  gen_server:call({rnd_exponential,[{alpha,Alpha}]}).
rnd_erlang(B,C) ->
  gen_server:call(?SERVER,{rnd_erlang,[{b,B},{c,C}]}).
rnd_uniform(Min,Max) ->
  gen_server:call(?SERVER,{rnd_uniform,[{min,Min},{max,Max}]}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
  {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([]) ->
  rand:seed(?RAND_ALGO,erlang:now()),
  {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
  {reply, Reply :: term(), NewState :: #state{}} |
  {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_call({rnd_exponential,{alpha,Alpha}}, _From, State) when  not is_float(Alpha); Alpha<0 ->
  {reply, {error,parameter_invalid}, State};
handle_call({rnd_exponential,[{alpha,Alpha}]}, _From, State) ->
  R = -((math:log(1-rand:uniform()))/Alpha),
  {reply, R, State};

handle_call({rnd_erlang,[{b,B},{c,C}]}, _From, State) when not is_integer(C) ; C<1; not is_float(B); B<0->
  {reply, {error,parameter_invalid}, State};
handle_call({rnd_erlang,[{b,B},{c,C}]}, _From, State) ->
  R  = generate_erlang_radnom_number(B,C),
  {reply, R, State};

handle_call({rnd_uniform,[{min,Min},{max,Max}]}, _From, State) when not is_float(Min); not is_float(Max);Max=<Min ->
  {reply, {error,parameter_invalid}, State};
handle_call({rnd_uniform,[{min,Min},{max,Max}]}, _From, State) ->
  R  = random:uniform()*(Max-Min) + Min,
  {reply, R, State};
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(_Request, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
  {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================


%%http://web.stanford.edu/group/uq/events/codes/Random.pdf
%%double erlang( double b, int c )
%%{
%%assert( b > 0. && c >= 1 );
%%double prod = 1.0;
%%for ( int i = 0; i < c; i++ ) prod *= uniform( 0., 1. );
%%return -b * log( prod );
%%}
generate_erlang_radnom_number(B,C) ->
  generate_erlang_radnom_number(B,C,1).
generate_erlang_radnom_number(B,1,Acc) ->
  -B * math:log(Acc);
generate_erlang_radnom_number(B,C,Acc) ->
  generate_erlang_radnom_number(B,C-1,Acc*rand:uniform()).


