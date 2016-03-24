%%%-------------------------------------------------------------------
%%% @author maciejtokarski
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 15. mar 2016 18:07
%%%-------------------------------------------------------------------
-module(mochitest).
-author("maciejtokarski").
-include("map_reduce.hrl").
-define(LOOP, {?MODULE, loop}).
%% API
-compile(export_all).
start() ->
  mochiweb_http:start([{name, ?MODULE}, {loop, ?LOOP},{nodelay, true} | [{port, 9900}]]).



loop(Req) ->
    "/" ++ Path = Req:get(path),
  try
    case Req:get(method) of
      Method when Method =:= 'GET'; Method =:= 'HEAD' ->
        case Path of
          "generate/" ++ What ->
            QueryStringData = Req:parse_qs(),
            handle_generate_request(Req, What, QueryStringData);
          _ ->
            Req:serve_file(Path, "./")
        end;
      'POST' ->
        case Path of
          _ ->
            Req:not_found()
        end;
      _ ->
        Req:respond({501, [], []})
    end
  catch
    Why:Err -> error_logger:info_msg("Exception ~p ~p", [Why, Err])
  end.



handle_generate_request(Req, "mrerlang.json", Proplist) ->
%%  StartWorkerProcessFun = fun() -> rnd_gen:start() end,
%%  StartWorkerProcessFun = fun() -> {ok,spawn('w@maciek-e450',rnd_gen,start,[])} end,

%%  StartWorkerProcessFun = fun() ->
%%                            Self = self(),
%%                            StartRemoteFun = fun() ->
%%                                                Result = (catch rnd_gen:start()),
%%                                                error_logger:info_msg("Result from start ~p",[Result]),
%%                                                Self ! Result
%%                                             end,
%%                            error_logger:info_msg("StartRemoteFun ~p",[StartRemoteFun]),
%%                            spawn('w@maciek-e450',StartRemoteFun),
%%                            receive
%%                              {ok,Pid} -> {ok,Pid};
%%                              Other -> error_logger:info_msg("Got other start result ~p",[Other])
%%                            end
%%                          end,
  StartWorkerProcessFun = fun() -> {ok,Pid} = rpc:call('w@maciek-e450',rnd_gen,start,[]),{ok,Pid} end,

  MapArgsFun = fun(_Args,WorkersCount) -> [50000  || _ <- lists:seq(0,WorkersCount-1)] end,
  WorkerFun = fun(WorkerPid,WorkerArg) -> rnd_gen:rnd_erlang(WorkerPid, 2.0, 5,WorkerArg,atom) end,
  ValidateFun = fun(WorkerResuult) -> erlang:is_list(WorkerResuult) end,
  Self = self(),
  FetchResultFun = fun(Result) -> Self ! Result end,
  TerminateProcessFun = fun(Pid) -> rnd_gen:stop(Pid) end,
  ReduceFun =      fun
                     (Result) when is_list(Result) -> {response,mochijson2:encode(lists:flatten(Result))};
                     (_) -> invalid
                   end,




  InitOpts = #m_r_init_opts{
    start_worker_process_fun = StartWorkerProcessFun,
    args = 'doesnt matter',
    worker_fun =  WorkerFun,
    workers_count =4,
    map_args_fun = MapArgsFun,
    validate_worker_result_fun = ValidateFun,
    reduce_fun = ReduceFun,
    fetch_result_fun = FetchResultFun,
    terminate_worker_process_fun = TerminateProcessFun,
%%    timeout  = 1000,
    autoterminate = true

  },
  {ok,MRPid} = map_reduce:start(InitOpts),
  map_reduce:go(MRPid),
  receive
    {reduce_timeout,_} -> send_series_http_response(reduce_timeout, Req);
    {timeout,_} -> send_series_http_response(timeout, Req);
    {response,Series} ->send_series_http_response(Series, Req);
    Other->
      error_logger:info_msg("Fetchfun other msg ~p",[Other]),
      send_series_http_response(unspecified, Req)
  end;


handle_generate_request(Req, "erlang.json", Proplist) ->
  {ok, Pid} = rnd_gen:start(),
  Series = (catch rnd_gen:rnd_erlang(Pid, 2.0, 5, 20000, json)),
  send_http_response_and_terminate_process(Series,Req,Pid);

handle_generate_request(Req, _, _) ->
  error_logger:error_info("~p",Req:dump()),
  Req:respond({400, [], "400 Bad Request\r\n"}).

send_http_response_and_terminate_process(Series,Req,GeneratorPid) ->
  send_series_http_response(Series, Req),
  terminate_generator_process(Series,GeneratorPid).


send_series_http_response(Series, Req) ->
  Response = case Series of
               Reply when is_list(Reply) ->
                 {200, [{"Content-Type", "application/json"}], [Reply]};
               {'EXIT', {timeout, _}} ->
                 error_logger:warning_msg("Got timeout message", []),
                 {503, [], "503 Timeout\r\n"};
               reduce_timeout ->
                 error_logger:error_msg("Got other message"),
                 {503, [], "503 Reduce Function Timeout\r\n"};
               timeout ->
                 error_logger:error_msg("Got other message"),
                 {503, [], "503 Worker Timeout\r\n"};
               Other ->
                 error_logger:error_msg("Got other message ~p",[Other]),
                 {503, [], "503 Unspecified error\r\n"}
             end,
  Req:respond(Response).

terminate_generator_process(Series, GenPid) ->
  case Series of
    Reply when is_list(Reply) ->
      rnd_gen:stop(GenPid);
    {'EXIT', {timeout, _}} ->
      erlang:exit(GenPid, kill);
    _ ->
      erlang:exit(GenPid, kill)
  end.