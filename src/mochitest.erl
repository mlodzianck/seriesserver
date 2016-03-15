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
  mochiweb_http:start([{name, ?MODULE}, {loop, ?LOOP} | [{port,9999}]]).



loop(Req) ->
    "/" ++ Path = Req:get(path),
  try
    case Req:get(method) of
      Method when Method =:= 'GET'; Method =:= 'HEAD' ->
        case Path of
          "generate/"++What ->
            io:format("~p",[What]),
            QueryStringData = Req:parse_qs(),
            handle_generate_request(Req,What,QueryStringData);
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
    Why:Err-> error_logger:info_msg("Exception ~p ~p",[Why,Err])
  end.


handle_generate_request(Req,"erlang.json", Proplist) ->
  R = [rnd_gen:rnd_erlang(2.0,1) || _X <- lists:seq(0,100000)],
  Req:respond({200, [{"Content-Type", "application/json"}], [mochijson2:encode(R)]});

handle_generate_request(Req,_,_) ->
  Req:respond({400, [], "400 Bad Request\r\n"}).