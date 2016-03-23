%%%-------------------------------------------------------------------
%%% @author maciek
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. mar 2016 14:58
%%%-------------------------------------------------------------------
-module(mrtest).
-author("maciek").
-compile(export_all).
-include("map_reduce.hrl").
%% API
-export([]).


test_() ->
  StartWorkerProcessFun = fun() ->
                              rnd_gen:start()
                          end,

  MapArgsFun = fun(_Args,WorkersCount) ->
                  [37000  || _ <- lists:seq(0,WorkersCount-1)]
               end,
  WorkerFun = fun(WorkerPid,WorkerArg) ->
                rnd_gen:rnd_erlang(WorkerPid, 2.0, 5,WorkerArg,atom)
              end,
  ValidateFun = fun(WorkerResuult) ->
                    erlang:is_list(WorkerResuult)
                end,
  PostprocessFun = fun
                     (Result) when is_list(Result) -> lists:flatten(Result);
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
    workers_count = 10,
    map_args_fun = MapArgsFun,
    validate_worker_result_fun = ValidateFun,
    post_process_result_fun = PostprocessFun,
    fetch_result_fun = FetchResultFun,
    terminate_worker_process_fun = TerminateProcessFun,
    worker_timeout = 1000

  },

  {ok,MRPid} = map_reduce:start(InitOpts),
  map_reduce:go(MRPid),
  receive
    {_,Any} when is_list(Any)-> error_logger:info_msg("========================>Got from MR with len ~p~n",[length(Any)]);
    _ -> error_logger:info_msg("========================>Got other reply from MR")
  end,
  map_reduce:stop(MRPid).

test() ->
  spawn(fun() -> test_() end).