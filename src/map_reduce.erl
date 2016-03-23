-module(map_reduce).
-author("maciek").

-behaviour(gen_server).

-export([start/1,go/1,stop/1]).

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

stop(Pid) ->
  gen_server:call(Pid,stop).
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

init([#m_r_init_opts{workers_count = WorkerCount,start_worker_process_fun = StartWorkerProcFun}=Opts]) ->
  process_flag(trap_exit, true),
  WorkerPids = [begin
                  {ok,Pid} = StartWorkerProcFun(),
                  Pid
                end || _X <- lists:seq(0,WorkerCount-1)],

  [link(WorkerPid) || WorkerPid <- WorkerPids ],
  {ok, #state{opts = Opts,worker_pids = WorkerPids}}.


handle_cast({go,RequestingPid}, #state{
                          opts = #m_r_init_opts{map_args_fun = MapArgsFun,
                                                args = Args,
                                                worker_fun =  WorkerFun,
                                                workers_count = WorkerCount,
                                                worker_timeout = WorkerTimeout
                                                },
                          worker_pids = WorkerPids
                      } = State
            )  ->
  WorkerArgsList = prepare_workers_args(MapArgsFun,Args,WorkerCount),
  error_logger:info_msg("Args prepared ~p~n",[WorkerArgsList]),
  Self=self(),
  WorkerFunWrapper  = fun(WorkerPid,WorkerArg) ->
                          WorkerRet =(catch WorkerFun(WorkerPid,WorkerArg)),
                          Self ! {worker_reply,WorkerPid,WorkerRet}
                      end,
  error_logger:info_msg("Spawning ~p workers~n",[WorkerCount]),


  [begin
     error_logger:info_msg("Spawning worker ~p~n",[WorkerIndex]),
     WorkerArg = lists:nth(WorkerIndex+1,WorkerArgsList),
     WorkerPid = lists:nth(WorkerIndex+1,WorkerPids),
     spawn(fun() -> WorkerFunWrapper(WorkerPid,WorkerArg) end),
     error_logger:info_msg("Spawned worker ~p~n",[WorkerIndex])
   end || WorkerIndex <- lists:seq(0,WorkerCount-1)],
  schedule_timeout_signal(Self,WorkerTimeout),
  {noreply,State#state{active_worker_pids = WorkerPids,requesting_pid = RequestingPid}}.

handle_call(stop,_,State) ->
  {stop,normal,ok,State};
handle_call(_,_,_) ->
  {error,not_implemented}.
handle_info({worker_reply,WorkerProcessPid,WorkerReply}, State = #state{active_worker_pids = [WorkerProcessPid],requesting_pid = RequestingPid,
                                                                        valid_replies = ValidReplies,
                                                                        invalid_replies = InvalidReplies,
                                                                        opts = #m_r_init_opts{validate_worker_result_fun = ResultValidateFun,
                                                                                              post_process_result_fun = PostprocessFun,
                                                                                              fetch_result_fun = FetchFun,
                                                                                              terminate_worker_process_fun = TerminateWorkerFun}}) ->
  NewActivePids = [],


  {IsReplyValid,NewValidReplies, NewInvalidReplies} =get_new_valid_and_invalid_replies(WorkerReply,ResultValidateFun,ValidReplies,InvalidReplies),
  case IsReplyValid of
    true -> ok;
    _-> error_logger:info_msg("Invalid worker response ~p",[WorkerReply])
  end,
  error_logger:info_msg("Last Pid reporting reply: valid ~p~n",[IsReplyValid]),
  clean_up_worker(IsReplyValid,TerminateWorkerFun,WorkerProcessPid),
  PostprocessedResult  =PostprocessFun(NewValidReplies),
  FetchFun(PostprocessedResult),

  {noreply, State#state{invalid_replies = NewInvalidReplies,valid_replies = NewValidReplies,active_worker_pids = NewActivePids}};


handle_info({worker_reply,WorkerProcessPid,WorkerReply}, State = #state{active_worker_pids = ActivePids,
                                                                        valid_replies = ValidReplies,
                                                                        invalid_replies = InvalidReplies,
                                                                        opts = #m_r_init_opts{validate_worker_result_fun = ResultValidateFun,
                                                                                              terminate_worker_process_fun = TerminateWorkerFun}}) ->
  NewActivePids = lists:delete(WorkerProcessPid,ActivePids),
  {IsReplyValid,NewValidReplies, NewInvalidReplies} =get_new_valid_and_invalid_replies(WorkerReply,ResultValidateFun,ValidReplies,InvalidReplies),
  case IsReplyValid of
    true -> ok;
    _-> error_logger:info_msg("Invalid worker response ~p",[WorkerReply])
  end,
  error_logger:info_msg("Pid ~p reported reply valid: ~p~n",[WorkerProcessPid,IsReplyValid]),

  clean_up_worker(IsReplyValid,TerminateWorkerFun,WorkerProcessPid),
  {noreply, State#state{invalid_replies = NewInvalidReplies,valid_replies = NewValidReplies,active_worker_pids = NewActivePids}};


handle_info({'EXIT',Pid,_},State = #state{worker_pids = WorkerPids}) ->
  error_logger:info_msg("Worker pid ~p reports die",[Pid]),
  {noreply, State#state{worker_pids = lists:delete(Pid,WorkerPids)}};
handle_info(worker_timeout, State=#state{active_worker_pids = []}) ->
  error_logger:info_msg("Got worker timeout but no worker is active right now"),
  {noreply, State};
handle_info(worker_timeout, State=#state{active_worker_pids = WorkerPids,valid_replies = ValidReplies,
                                          opts = #m_r_init_opts{post_process_result_fun = PostprocessFun,fetch_result_fun = FetchFun}}) ->
  error_logger:info_msg("Got worker timeoutwith active ~p workers ~n",[length(WorkerPids)]),

  [ begin
      case erlang:process_info(WorkerProcessPid) of
        undefined -> ok;
        _->erlang:exit(WorkerProcessPid,kill)
      end
    end
     || WorkerProcessPid <- WorkerPids
  ],
  {noreply, State};
handle_info(Info, State) ->
  error_logger:info_msg("Got info ~p~n",[Info]),
  {noreply, State}.


terminate(_Reason, State = #state{worker_pids = WorkerPids}) ->
  error_logger:info_msg("Exiting due to  ~p with worker pids ~p ~n",[_Reason,WorkerPids]),
  ok.
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

prepare_workers_args(MapArgsFun,Args,WorkersCount) ->
  MapArgsFun(Args,WorkersCount).

get_new_valid_and_invalid_replies(WorkerReply,ResultValidateFun,ValidReplies,InvalidReplies) ->
  case ResultValidateFun(WorkerReply) of
      true->
        {true,ValidReplies ++ [WorkerReply],InvalidReplies};
      _->
        {false,ValidReplies,InvalidReplies ++ [WorkerReply]}
    end.

clean_up_worker(_,TerminateWorkerFun,WorkerProcessPid) when is_function(TerminateWorkerFun,1) ->
  error_logger:info_msg("Killing pid with terminate fun ~p~n",[WorkerProcessPid]),
  catch TerminateWorkerFun(WorkerProcessPid);
clean_up_worker(_,_,WorkerProcessPid)  ->
  error_logger:info_msg("Killing pid with kill signal ~p~n",[WorkerProcessPid]),
  erlang:exit(WorkerProcessPid,kill).

schedule_timeout_signal(_,infinity) ->
  ok;
schedule_timeout_signal(Pid,Timeout) ->
  erlang:send_after(Timeout,Pid,worker_timeout).
