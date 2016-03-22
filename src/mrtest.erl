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


test() ->
  StartWorkerProcessFun = fun() ->
                              rnd_gen:start()
                          end,

  MapArgsFun = fun(Args,WorkersCount) ->
                  [100000  || _ <- lists:seq(0,WorkersCount-1)]
               end,
  WorkerFun = fun(WorkerPid,WorkerArg) ->
                io:format("Worker fun entry~n"),
                Ret = rnd_gen:rnd_erlang(WorkerPid, 2.0, 5,WorkerArg,atom,1000),
                io:format("Worker fun exit~n"),
                Ret
              end,
  ValidateFun = fun(WorkerResuult) ->
                    is_list(WorkerResuult)
                end,



  InitOpts = #m_r_init_opts{
    start_worker_process_fun = StartWorkerProcessFun,
    worker_module = rnd_gen,
    args = 'doesnt matter',
    worker_fun =  WorkerFun,
    workers_count = 10,
    map_args_fun = MapArgsFun,
    validate_worker_result_fun = ValidateFun

  },

  {ok,MRPid} = map_reduce:start(InitOpts),
  map_reduce:go(MRPid),
  receive
    Any -> error_logger:info_msg("Got from MR~n",[])
  end.