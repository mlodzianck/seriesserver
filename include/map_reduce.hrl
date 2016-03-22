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
  map_args_fun,
  workers_count = 4,
  worker_module,
  worker_fun,
  validate_worker_result_fun,
  reduce_fun,
  post_process_result_fun,
  initial_result_value}).
