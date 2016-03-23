%%%-------------------------------------------------------------------
%%% @author maciek
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. mar 2016 14:58
%%%-------------------------------------------------------------------
-module(mr).
-author("maciek").
-compile(export_all).
-include("map_reduce.hrl").
%% API
-export([]).


t() ->
  StartWorkerProcessFun = fun() ->
                              rnd_gen:start()
                          end,

  MapArgsFun = fun(_Args,WorkersCount) ->
                  [50000  || _ <- lists:seq(0,WorkersCount-1)]
               end,
  WorkerFun = fun(WorkerPid,WorkerArg) ->
                rnd_gen:rnd_erlang(WorkerPid, 2.0, 5,WorkerArg,atom)
              end,
  ValidateFun = fun(WorkerResuult) ->
                    erlang:is_list(WorkerResuult)
                end,
  PostprocessFun = fun
                     (Result) when is_list(Result) -> mochijson2:encode(lists:flatten(Result));
                     (_) -> []
                   end,


  Self = self(),
  FetchResultFun = fun(Result) ->
                      Self ! {m_r_result,Result}
                      end,
  TerminateProcessFun = fun(Pid) ->
                          rnd_gen:stop(Pid)
                          end,



  InitOpts = #m_r_init_opts{
    start_worker_process_fun = StartWorkerProcessFun,
    args = 'doesnt matter',
    worker_fun =  WorkerFun,
    workers_count =2,
    map_args_fun = MapArgsFun,
    validate_worker_result_fun = ValidateFun,
    reduce_fun = PostprocessFun,
    fetch_result_fun = FetchResultFun,
    terminate_worker_process_fun = TerminateProcessFun,
    worker_timeout = 4000

  },

  {ok,MRPid} = map_reduce:start(InitOpts),
  map_reduce:go(MRPid),
  receive
    {_,{reduce_timeout,_}} ->error_logger:info_msg("========================>Reduce timeout!!!");
    {_,{timeout,_}} ->error_logger:info_msg("========================>Worker timeout!!!");
    {_,Any} when is_list(Any)-> error_logger:info_msg("========================>Got from MR with len ~n",[]);
    _ -> error_logger:info_msg("========================>Got other reply from MR")
  end,
  map_reduce:stop(MRPid).
