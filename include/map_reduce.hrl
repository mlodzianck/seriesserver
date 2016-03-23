%%%-------------------------------------------------------------------
%%% @author maciek
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. mar 2016 11:22
%%%-------------------------------------------------------------------
-author("maciek").


-record(m_r_init_opts,{args,
  start_worker_process_fun,
  worker_fun,
  worker_timeout = infinity,
  map_args_fun,
  reduce_fun,
  workers_count = 4,
  validate_worker_result_fun,
  post_process_result_fun,
  fetch_result_fun,
  terminate_worker_process_fun}).
