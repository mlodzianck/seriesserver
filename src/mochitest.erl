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
-define(LOOP, {?MODULE, loop}).
%% API
-compile(export_all).
start() ->
  mochiweb_http:start([{name, ?MODULE}, {loop, ?LOOP} | [{port, 9900}]]).



loop(Req) ->
    "/" ++ Path = Req:get(path),
  try
    case Req:get(method) of
      Method when Method =:= 'GET'; Method =:= 'HEAD' ->
        case Path of
          "generate/" ++ What ->
            io:format("~p", [What]),
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


handle_generate_request(Req, "erlang.json", Proplist) ->
  {ok, Pid} = rnd_gen:start(),
  Series = (catch rnd_gen:rnd_erlang(Pid, 2.0, 5, 10000000, json, 1000)),
  send_http_response_and_terminate_process(Series,Req,Pid);


handle_generate_request(Req, _, _) ->
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
               Other ->
                 error_logger:error_msg("Got other message ~p", [Other]),
                 {503, [], "503 Timeout\r\n"}
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