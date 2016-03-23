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

-record(state, {opts,
  worker_pids=[],
  active_worker_pids=[],
  valid_replies=[],
  invalid_replies=[],
  requesting_pid,
  timeout_timer = undefined,
  reduce_pid}).
-include("map_reduce.hrl").


start(#m_r_init_opts{}=InitOpts) ->
  gen_server:start(?MODULE, [InitOpts], []).

go(Pid) ->
  RequestorPid = self(),
  gen_server:cast(Pid,{go,RequestorPid}).

stop(Pid) ->
  gen_server:call(Pid,stop).

init([#m_r_init_opts{workers_count = WorkerCount,start_worker_process_fun = StartWorkerProcFun}=Opts]) ->
  process_flag(trap_exit, true),
  WorkerPids = [begin
                  {ok,Pid} = StartWorkerProcFun(),
                  Pid
                end || _ <- lists:seq(0,WorkerCount-1)],

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
  Self=self(),
  WorkerFunWrapper  = fun(WorkerPid,WorkerArg) ->
                          WorkerRet =(catch WorkerFun(WorkerPid,WorkerArg)),
                          gen_server:cast(Self,{worker_reply,WorkerPid,WorkerRet})
                      end,

  [begin
     WorkerArg = lists:nth(WorkerIndex+1,WorkerArgsList),
     WorkerPid = lists:nth(WorkerIndex+1,WorkerPids),
     spawn(fun() -> WorkerFunWrapper(WorkerPid,WorkerArg) end)
   end || WorkerIndex <- lists:seq(0,WorkerCount-1)],
  TimeoutTimerRef = schedule_timeout_signal(Self,WorkerTimeout),
  {noreply,State#state{active_worker_pids = WorkerPids,requesting_pid = RequestingPid,timeout_timer = TimeoutTimerRef}};

handle_cast({worker_reply,WorkerProcessPid,WorkerReply}, State = #state{active_worker_pids = [WorkerProcessPid],timeout_timer = undefined,
                                                                        opts = #m_r_init_opts{fetch_result_fun = FetchFun}}) ->

  {NewValidReplies,NewInvalidReplies,NewActivePids} = handle_worker_reply(WorkerProcessPid,WorkerReply,State),
  error_logger:info_msg("Time is up, can't do reduce because timeout has expired"),
  spawn(fun() -> FetchFun({timeout,NewValidReplies}) end),
  NewState =State#state{invalid_replies = NewInvalidReplies,valid_replies = NewValidReplies,active_worker_pids = NewActivePids},
  {noreply,NewState};


handle_cast({worker_reply,WorkerProcessPid,WorkerReply}, State = #state{active_worker_pids = [WorkerProcessPid],
                                                                        opts = #m_r_init_opts{reduce_fun  = ReduceFun}}) ->
  {NewValidReplies,NewInvalidReplies,NewActivePids} = handle_worker_reply(WorkerProcessPid,WorkerReply,State),
  Self = self(),
  ReduceWrapper = fun() ->
    Result = ReduceFun(NewValidReplies),
    gen_server:cast(Self,{reduce_result,Result})
  end,
  ReducePid = spawn(ReduceWrapper),
  link(ReducePid),
  error_logger:info_msg("Reduce spawned with pid ~p",[ReducePid]),
  NewState =State#state{invalid_replies = NewInvalidReplies,valid_replies = NewValidReplies,active_worker_pids = NewActivePids,reduce_pid = ReducePid},
  {noreply,NewState};

handle_cast({worker_reply,WorkerProcessPid,WorkerReply}, State) ->
  {NewValidReplies,NewInvalidReplies,NewActivePids} = handle_worker_reply(WorkerProcessPid,WorkerReply,State),
  NewState = State#state{invalid_replies = NewInvalidReplies,valid_replies = NewValidReplies,active_worker_pids = NewActivePids},
  {noreply, NewState};

handle_cast({reduce_result,Result},State = #state{opts = #m_r_init_opts{
                                                  fetch_result_fun = FetchFun},timeout_timer  = TimerRef}) ->
  error_logger:info_msg("Reduce result on time!"),
  case TimerRef of
    undefined -> ok;
    _ -> erlang:cancel_timer(TimerRef)
  end,
  spawn(fun() -> FetchFun(Result) end),
  {noreply,State#state{timeout_timer = undefined}}.

handle_call(stop,_,State) ->
  {stop,normal,ok,State}.


handle_info({'EXIT',Pid,_},State = #state{reduce_pid = Pid}) ->
  error_logger:info_msg("Reduce pid ~p reports exit",[Pid]),
  {noreply, State#state{reduce_pid = undefined}};

handle_info({'EXIT',Pid,_},State = #state{worker_pids = WorkerPids}) ->
  error_logger:info_msg("Worker pid ~p reports die",[Pid]),
  {noreply, State#state{worker_pids = lists:delete(Pid,WorkerPids)}};



handle_info(timeout, State=#state{active_worker_pids = [],opts = #m_r_init_opts{fetch_result_fun = FetchFun},valid_replies = V,reduce_pid = ReducePid}) when is_pid(ReducePid) ->
  error_logger:info_msg("Reduce timeout!"),
  spawn(fun() -> FetchFun({reduce_timeout,V}) end),
  erlang:exit(ReducePid,kill),
  {noreply, State#state{timeout_timer  = undefined,reduce_pid = undefined}};

handle_info(timeout, State=#state{active_worker_pids = []}) ->
  error_logger:info_msg("Got worker timeout but no worker is active right now"),
  {noreply, State};

handle_info(timeout, State=#state{active_worker_pids = WorkerPids}) ->
  error_logger:info_msg("Got worker timeout with active ~p workers ~n",[length(WorkerPids)]),

  [ begin
      case erlang:process_info(WorkerProcessPid) of
        undefined -> ok;
        _->erlang:exit(WorkerProcessPid,kill)
      end
    end
     || WorkerProcessPid <- WorkerPids
  ],
  {noreply, State#state{timeout_timer = undefined}};

handle_info(Info, State) ->
  error_logger:info_msg("Got info ~p~n",[Info]),
  {noreply, State}.


terminate(_Reason, #state{worker_pids = WorkerPids,valid_replies = V, invalid_replies = IV}) ->
  error_logger:info_msg("Exiting due to  ~p with worker pids ~p ~n",[_Reason,WorkerPids]),
  error_logger:info_msg("Valid responses count ~p invalid ~p",[length(V),length(IV)]),
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
  catch TerminateWorkerFun(WorkerProcessPid);
clean_up_worker(_,_,WorkerProcessPid)  ->
  erlang:exit(WorkerProcessPid,kill).

schedule_timeout_signal(_,infinity) ->
  undefined;
schedule_timeout_signal(Pid,Timeout) ->
  erlang:send_after(Timeout,Pid,timeout).


handle_worker_reply(WorkerProcessPid,WorkerReply, #state{active_worker_pids = ActivePids,
                                                                  valid_replies = ValidReplies,
                                                                  invalid_replies = InvalidReplies,
                                                                  opts = #m_r_init_opts{validate_worker_result_fun = ResultValidateFun,
                                                                  terminate_worker_process_fun = TerminateWorkerFun}}) ->
  NewActivePids = lists:delete(WorkerProcessPid,ActivePids),
  {IsReplyValid,NewValidReplies, NewInvalidReplies} =get_new_valid_and_invalid_replies(WorkerReply,ResultValidateFun,ValidReplies,InvalidReplies),
  error_logger:info_msg("Pid ~p reported reply, is valid?: ~p~n",[WorkerProcessPid,IsReplyValid]),

  clean_up_worker(IsReplyValid,TerminateWorkerFun,WorkerProcessPid),
  {NewValidReplies,NewInvalidReplies,NewActivePids}.