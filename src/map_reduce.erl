-module(map_reduce).
-author("maciek").

-behaviour(gen_server).

-export([start/1,go/1]).

-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {opts,worker_pids=[]}).
-include("map_reduce.hrl").


start(#m_r_init_opts{}=InitOpts) ->
  gen_server:start(?MODULE, [InitOpts], []).

go(Pid) ->
  RequestorPid = self(),
  gen_server:cast(Pid,{go,RequestorPid}).

%%init([#m_r_init_opts{
%%      map_args_fun = MapArgsFun,
%%      reduce_fun = ReduceFun,
%%      validate_worker_result_fun = ValidateWorkerResultFun,
%%      workers_count = WorkersCount,
%%      worker_fun=WorkerFun,
%%      worker_module = WorkerModule,
%%      post_process_result_fun = PostProcessResultFun}])
%%  when not is_function(MapArgsFun,2) ;
%%    not is_function(ReduceFun,2) ;
%%    not is_function(ValidateWorkerResultFun) ;
%%    not is_integer(WorkersCount);
%%    WorkersCount < 1;
%%    not is_function(WorkerFun,2);
%%    not is_function(PostProcessResultFun,1);
%%    not is_atom(WorkerModule)->
%%  {error , invalid_init_opts};

init([#m_r_init_opts{worker_module = WorkerMod,workers_count = WorkerCount}=Opts]) ->
  WorkerPids = [spawn_worker_module(WorkerMod) || _X <- lists:seq(0,WorkerCount-1)],
  process_flag(trap_exit,true),
  {ok, #state{opts = Opts,worker_pids = WorkerPids}}.


handle_cast({go,RequestingPid}, #state{
                          opts = #m_r_init_opts{map_args_fun = MapArgsFun,
                                                args = Args,
                                                post_process_result_fun = PostproessFun,
                                                worker_fun =  WorkerFun,
                                                workers_count = WorkerCount,
                                                validate_worker_result_fun = ValidateWorkerResultFun
                                                },
                          worker_pids = WorkerPids
                      } = State
            )  ->
  WorkerArgsList = prepare_workers_args(MapArgsFun,Args,WorkerCount),
  io:format("Args prepared ~p~n",[WorkerArgsList]),
  Self=self(),
  WorkerFunWrapper  = fun(WorkerPid,WorkerArg) ->
                          WorkerRet = (catch WorkerFun(WorkerPid,WorkerArg)),
                          Self ! {worker_reply,WorkerPid,WorkerRet}
                      end,
  io:format("Spawning ~p workers~n",[WorkerCount]),

  SpawnWorkerFun = fun(WorkerIndex) ->
    io:format("Spawning worker ~p~n",[WorkerIndex]),
    WorkerArg = lists:nth(WorkerIndex+1,WorkerArgsList),
    WorkerPid = lists:nth(WorkerIndex+1,WorkerPids),

    spawn(fun() -> WorkerFunWrapper(WorkerPid,WorkerArg) end),
    io:format("Spawned worker ~p~n",[WorkerIndex])
  end,
  [SpawnWorkerFun(WorkerIndex)|| WorkerIndex <- lists:seq(0,WorkerCount-1)],

%%  WorkersReturnList =[
%%    receive
%%      {worker_reply,WorkerPid,WorkerRet} ->
%%        case ValidateWorkerResultFun(WorkerRet) of
%%          true ->
%%            io:format("Got valid worker result ~p, pid ~p~n",[WorkerRet,WorkerPid]),
%%            WorkerRet;
%%          _->
%%            io:format("Got invalid worker result ~p, killing pid ~p~n",[WorkerRet,WorkerPid]),
%%            erlang:exit(WorkerPid, kill),
%%            {invalid,WorkerRet}
%%        end
%%    end || _ <- lists:seq(0,WorkerCount-1)
%%  ],
%%
%%
%%  RequestingPid ! {m_r_reply,WorkersReturnList},


  {noreply,State}.


handle_call(_,_,_) ->
  {error,not_implemented}.

handle_info(Info, State) ->
  io:format("Got info ~p~n",[Info]),
  {noreply, State}.


terminate(_Reason, _State) ->
  io:format("Exiting due to  ~p~n",[_Reason]),
  ok.
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

spawn_worker_module(ModuleName) ->
  {ok,WorkerPid} = ModuleName:start(),
  link(WorkerPid),
  WorkerPid.
prepare_workers_args(MapArgsFun,Args,WorkersCount) ->
  MapArgsFun(Args,WorkersCount).