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

-record(state, {opts,worker_pids=[],
  pids_valid_reply=[],
  pids_invalid_reply=[],
  active_worker_pids=[],
  valid_replies=[],
  invalid_replies=[],
  requesting_pid}).
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

init([#m_r_init_opts{worker_module = WorkerMod,workers_count = WorkerCount,start_worker_process_fun = StartWorkerProcFun}=Opts]) ->
  process_flag(trap_exit,true),
  WorkerPids = [begin
                  {ok,Pid} = StartWorkerProcFun(),
                  Pid
                end|| _X <- lists:seq(0,WorkerCount-1)],

  [link(WorkerPid) || WorkerPid <- WorkerPids ],
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
                          WorkerRet =(catch WorkerFun(WorkerPid,WorkerArg)),
                          %WorkerRet =(WorkerFun(WorkerPid,WorkerArg)),
                          Self ! {worker_reply,WorkerPid,WorkerRet}
                      end,
  io:format("Spawning ~p workers~n",[WorkerCount]),


  [begin
     io:format("Spawning worker ~p~n",[WorkerIndex]),
     WorkerArg = lists:nth(WorkerIndex+1,WorkerArgsList),
     WorkerPid = lists:nth(WorkerIndex+1,WorkerPids),
     spawn(fun() -> WorkerFunWrapper(WorkerPid,WorkerArg) end),
     io:format("Spawned worker ~p~n",[WorkerIndex])
   end || WorkerIndex <- lists:seq(0,WorkerCount-1)],
  {noreply,State#state{active_worker_pids = WorkerPids,requesting_pid = RequestingPid}}.


handle_call(_,_,_) ->
  {error,not_implemented}.

handle_info({worker_reply,WorkerProcessPid,WorkerReply}, State = #state{active_worker_pids = ActivePids,
                                                                        valid_replies = ValidReplies,
                                                                        invalid_replies = InvalidReplies,
                                                                        opts = #m_r_init_opts{validate_worker_result_fun = ResultValidateFun}}) ->
  NewActivePids = lists:delete(WorkerProcessPid,ActivePids),
  case ResultValidateFun(WorkerReply) of
      true->
  end;


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