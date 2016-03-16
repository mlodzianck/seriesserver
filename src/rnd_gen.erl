-module(rnd_gen).
-author("maciek").

-behaviour(gen_server).

%% API


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
-export([start/0]).
-export([rnd_expotential/4,rnd_erlang/5,rnd_uniform/5]).
-export([rnd_expotential/5,rnd_erlang/6,rnd_uniform/6]).
-export([stop/1]).


start() ->
  gen_server:start(?MODULE, [], []).
rnd_expotential(Pid,Alpha,SeriesLen,Format) ->
  rnd_expotential(Pid,Alpha,SeriesLen,Format,infinity).

rnd_erlang(Pid,B,C,SeriesLen,Format) ->
  rnd_erlang(Pid,B,C,SeriesLen,Format,infinity).

rnd_uniform(Pid,Min,Max,Format,SeriesLen) ->
  rnd_uniform(Pid,Min,Max,SeriesLen,Format,infinity).


rnd_expotential(Pid,Alpha,SeriesLen,Format,Timeout) ->
  gen_server:call(Pid,{rnd_exponential,[{alpha,Alpha}],Format,SeriesLen},Format,Timeout).

rnd_erlang(Pid,B,C,SeriesLen,Format,Timeout) ->
  gen_server:call(Pid,{rnd_erlang,[{b,B},{c,C}],Format,SeriesLen},Timeout).

rnd_uniform(Pid,Min,Max,SeriesLen,Format,Timeout) ->
  gen_server:call(Pid,{rnd_uniform,[{min,Min},{max,Max}],Format,SeriesLen},Timeout).

stop(Pid) ->
  gen_server:call(Pid,stop).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


init([]) ->
  rand:seed(?RAND_ALGO,erlang:now()),
  {ok, #state{}}.


handle_call({rnd_exponential,{alpha,Alpha},_,_}, _From, State) when  not is_float(Alpha); Alpha<0 ->
  {reply, {error,parameter_invalid}, State};
handle_call({rnd_exponential,[{alpha,Alpha}],Format,SeriesLen}, _From, State) ->
  R = [-((math:log(1-rand:uniform()))/Alpha) || _X <- lists:seq(0,SeriesLen)],
  {reply, format_convert(R,Format), State};


handle_call({rnd_erlang,[{b,B},{c,C}],_,_}, _From, State) when not is_integer(C) ; C<1; not is_float(B); B<0->
  {reply, {error,parameter_invalid}, State};
handle_call({rnd_erlang,[{b,B},{c,C}],Format,SeriesLen}, _From, State) ->
  R  = [generate_erlang_radnom_number(B,C) || _X <- lists:seq(0,SeriesLen)],
  {reply, format_convert(R,Format), State};

handle_call({rnd_uniform,[{min,Min},{max,Max}],_,_}, _From, State) when not is_float(Min); not is_float(Max);Max=<Min ->
  {reply, {error,parameter_invalid}, State};
handle_call({rnd_uniform,[{min,Min},{max,Max}],Format,SeriesLen}, _From, State) ->
  R  = [random:uniform()*(Max-Min) + Min || _X <- lists:seq(0,SeriesLen)],
  {reply, format_convert(R,Format), State};

handle_call(stop, _From, State) ->
  {stop,normal,ok,State};

handle_call(_Request, _From, State) ->
  {reply, ok, State}.


handle_cast(_Request, State) ->
  {noreply, State}.


handle_info(_Info, State) ->
  {noreply, State}.


terminate(Reason, _State) ->
  error_logger:info_msg("Terminating rnd_gen server due to ~p",[Reason]),
  ok.


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

format_convert(Series,Format) ->
  case Format of
    json ->  mochijson2:encode(Series);
    _ ->  Series
  end.