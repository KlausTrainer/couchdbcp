%%% This file is part of the CouchDBCP package and is released under the
%%% Tumbolia Public License. See LICENSE for more details.
%%%
%%% @author Klaus Trainer <klaus.trainer@web.de>

%%% @doc CouchDBCP web server module.

-module(couchdbcp_web).
-author('Klaus Trainer <klaus.trainer@web.de>').

-include("couchdbcp.hrl").

%% user interface
-export([start/1, stop/0, loop/2]).
%% intermodule exports
-export([get_db_and_doc_name/1, make_header_list/3, make_url/2]).

-define(IBROWSE_OPTIONS, [{response_format, binary}, {connect_timeout, 5000},
    {inactivity_timeout, infinity}]).
-define(TIMEOUT, 5000).

%% External API

start(Options) ->
    {DocRoot, Options1} = get_option(docroot, Options),
    Loop = fun(Req) -> ?MODULE:loop(Req, DocRoot) end,
    mochiweb_http:start([{name, ?MODULE}, {loop, Loop} | Options1]).

stop() ->
    mochiweb_http:stop(?MODULE).

loop(Req, _DocRoot) ->
    % translate mochiweb to ibrowse method names
    Method = case Req:get(method) of
             "COPY" -> copy; % sic!
             'DELETE' -> delete;
             'GET' -> get;
             'HEAD' -> head;
             'POST' -> post;
             'PUT' -> put
             end,
    ThisCouch = couchdbcp:get_app_env(this_couch),
    Headers = Req:get(headers),
    RawPath = Req:get(raw_path),
    ReqParams = Req:parse_qs(),
    Cookie = Req:get_cookie_value("AuthSession"),
    IfNoneMatch = Req:get_header_value("If-None-Match"),
    Consistency = Req:get_header_value("X-CouchDBCP-Consistency"),
    CouchDBCP_Tell = Req:get_header_value("X-CouchDBCP-Tell"),
    CouchDBCP_Write = Req:get_header_value("X-CouchDBCP-Write"),
    CouchDBCP_Replicate = Req:get_header_value("X-CouchDBCP-Replicate"),
    CouchDBCP_Replicated = Req:get_header_value("X-CouchDBCP-Replicated"),
    CouchDBCP_SetCookie = Req:get_header_value("X-CouchDBCP-Set-Cookie"),
    CouchDBCP_UnsetCookie = Req:get_header_value("X-CouchDBCP-Unset-Cookie"),
    ReadConsistency =
        case Consistency of
        undefined -> couchdbcp:get_app_env(couchdbcp_read_consistency);
        R -> ?l2a(R)
        end,
    WriteConsistency =
        case Consistency of
        undefined -> couchdbcp:get_app_env(couchdbcp_write_consistency);
        W -> ?l2a(W)
        end,
    case Method of
    Method when (Method =:= get orelse Method =:= head) andalso IfNoneMatch =:= undefined ->
        {DB, DocName} = get_db_and_doc_name(RawPath),
        DB_ = string:substr(DB, 1, 1) =:= "_",
        Local = string:substr(DocName, 1, 6) =:= "_local",
        Eventual = DocName =:= [] orelse Local orelse DocName =:= "_all_docs" orelse DocName =:= "_changes" orelse DocName =:= "_session",
        if
        ReadConsistency =:= eventual; DB_; Eventual ->
            case handle_read_request(Req, ThisCouch, RawPath, make_header_list(Headers, Cookie, ThisCouch), Method) of
            {error, _Reason} -> gen_tcp:close(Req:get(socket));
            _ -> ok
            end;
        ReadConsistency =:= atomic ->
            case couchdbcp:check_read_quorum(RawPath, Cookie) of
            {error, Code} ->
                Req:respond({Code, [], []});
            {ok, {_ResCode, ETag, ResHeader, Addr}} ->
                case Method of
                head ->
                    send_header(Req, 200, ResHeader);
                _ ->
                    RawPath1 = add_rev_to_raw_path(ReqParams, RawPath, ETag),
                    case handle_read_request(Req, Addr, RawPath1, make_header_list(Headers, Cookie, Addr), Method) of
                    {error, _Reason} -> gen_tcp:close(Req:get(socket));
                    _ -> ok
                    end
                end
            end
        end;
    Method when Method =:= get; Method =:= head ->
        {DB, DocName} = get_db_and_doc_name(RawPath),
        DB_ = string:substr(DB, 1, 1) =:= "_",
        Local = string:substr(DocName, 1, 6) =:= "_local",
        Eventual = DocName =:= [] orelse Local orelse DocName =:= "_all_docs" orelse DocName =:= "_changes" orelse DocName =:= "_session",
        if
        ReadConsistency =:= eventual; DB_; Eventual ->
            case handle_read_request(Req, ThisCouch, RawPath, make_header_list(Headers, Cookie, ThisCouch), Method) of
            {error, _Reason} -> gen_tcp:close(Req:get(socket));
            _ -> ok
            end;
        ReadConsistency =:= atomic ->
            case couchdbcp:check_read_quorum(RawPath, Cookie) of
            {error, Code} ->
                Req:respond({Code, [], []});
            {ok, {_ResCode, ETag, ResHeader, Addr}} ->
                case ETag of
                IfNoneMatch ->
                    send_header(Req, 304, ResHeader);
                _ when Method =:= head ->
                    send_header(Req, 200, ResHeader);
                _ ->
                    RawPath1 = add_rev_to_raw_path(ReqParams, RawPath, ETag),
                    case handle_read_request(Req, Addr, RawPath1, make_header_list(Headers, Cookie, Addr), Method) of
                    {error, _Reason} -> gen_tcp:close(Req:get(socket));
                    _ -> ok
                    end
                end
            end
        end;
    post when CouchDBCP_Tell =/= undefined ->
        [Server, Port] = string:tokens(CouchDBCP_Tell, ":"),
        DeadProxy = {Server, ?l2i(Port)},
        Pid = couchdbcp:get_app_env({peer_notifier, DeadProxy}),
        Pid ! {tell, string:substr(RawPath, 2)},
        Req:respond({202, [], []});
    post when CouchDBCP_Replicate =/= undefined ->
        DB = string:substr(RawPath, 2),
        case rpc(replication_status_store, {get, DB}) of
        true ->
            ok;
        undefined ->
            replication_status_store ! {put, DB},
            {User, Password} = couchdbcp:get_app_env(couchdbcp_admin_authorization),
            Auth = User ++ ":" ++ Password ++ "@",
            SourceUrl = ?l2b("http://" ++ Auth ++ CouchDBCP_Replicate ++ RawPath),
            {Domain, Port} = ThisCouch,
            TargetUrl = ?l2b("http://" ++ Auth ++ Domain ++ ":" ++ ?i2l(Port) ++ "/" ++ DB),
            Body = <<"{\"source\":\"",SourceUrl/binary,"\",\"target\":\"",TargetUrl/binary,"\",\"create_target\":true}">>,
            Headers1 = mochiweb_headers:enter("Content-Type",
                "application/json", Headers),
            case handle_write_request(Req, ThisCouch, "/_replicate", make_header_list(Headers1, Cookie, ThisCouch), post, Body) of
            {error, _Reason} ->
                error_logger:info_msg("Could not update ~p.~n", [DB]),
                gen_tcp:close(Req:get(socket));
            _ ->
                couchdbcp:notify_replication_success_to_peers(DB),
                error_logger:info_msg("Updated ~p.~n", [DB])
            end,
            replication_status_store ! {erase, DB}
        end;
    post when CouchDBCP_Replicated =/= undefined ->
        [Server, Port] = string:tokens(CouchDBCP_Replicated, ":"),
        Sender = {Server, ?l2i(Port)},
        Pid = couchdbcp:get_app_env({peer_notifier, Sender}),
        Pid ! {has_replicated, string:substr(RawPath, 2)},
        Req:respond({200, [], []});
    post when CouchDBCP_SetCookie =/= undefined ->
        {Cookie1, Cookie2} = ?b2t(Req:recv_body()),
        case Cookie1 of
        [] -> ok;
        _ -> term_cache:put(couchdbcp:get_app_env(cookie_store), Cookie1, Cookie2)
        end,
        Req:respond({200, [], []});
    post when CouchDBCP_UnsetCookie =/= undefined ->
        term_cache:erase(couchdbcp:get_app_env(cookie_store),
            CouchDBCP_UnsetCookie),
        Req:respond({200, [], []});
    Method when Method =:= delete; Method =:= copy; Method =:= post; Method =:= put ->
        Body = case Method of
               Method when Method =:= post; Method =:= put ->
                   case Req:recv_body() of
                   undefined -> <<>>;
                   B -> B
                   end;
               Method when Method =:= copy; Method =:= delete ->
                   <<>>
               end,
        {_DB, DocName} = get_db_and_doc_name(RawPath),
        Local = string:substr(DocName, 1, 6) =:= "_local",
        case CouchDBCP_Write of
        undefined when Local =:= false ->
            case couchdbcp:write(WriteConsistency, RawPath, Cookie, Headers, Method, Body) of
            {error, Code} ->
                Req:respond({Code, [], []});
            {ok, {ResCode, ResHeaderList, Body1}} ->
                case lists:keyfind("Transfer-Encoding", 1, ResHeaderList) of
                {_, "chunked"} ->
                    ResHeaderList1 = lists:keydelete("Transfer-Encoding", 1,
                        ResHeaderList),
                    ResHeaderList2 = lists:keystore("Content-Length", 1,
                        ResHeaderList1, {"Content-Length", byte_size(Body1)});
                _ ->
                    ResHeaderList2 = ResHeaderList
                end,
                ResHeaderList3 = replace_location_header(ResHeaderList2, Req),
                Req:respond({ResCode, ResHeaderList3, Body1})
            end;
        _ ->
            case handle_write_request(Req, ThisCouch, RawPath, make_header_list(Headers, Cookie, ThisCouch), Method, Body) of
            {error, _Reason} -> gen_tcp:close(Req:get(socket));
            _ -> ok
            end
        end;
    _ ->
        Req:respond({501, [], []})
    end.

%% @spec get_db_and_doc_name(string()) -> {string(), string()}
%% @doc Returns database and document name, or an empty list, respectively.
get_db_and_doc_name(RawPath) ->
    L = string:tokens(RawPath, "/?"),
    case length(L) of
    0 ->
        {[], []};
    1 ->
        [L1|_] = L,
        {L1, []};
    _ ->
        [L1, L2|_] = L,
        {L1, L2}
    end.

%% @spec make_header_list(Headers::headers(), Cookie::string(), Addr::address()) -> header_list()
%% @doc Makes a `header_list()' from `headers()' and replaces the AuthSession Cookie if necessary.
make_header_list(Headers, Cookie, Addr) ->
    case couchdbcp:get_app_env(this_couch) of
    Addr when Cookie =/= undefined andalso Cookie =/= [] ->
        CookieHeader = mochiweb_headers:get_value("Cookie", Headers),
        Headers1 = 
            case term_cache:get(couchdbcp:get_app_env(cookie_store), Cookie) of
            not_found ->
                Headers;
            {ok, Cookie1} ->
                CookieHeader1 = re:replace(CookieHeader, Cookie, Cookie1,
                    [{return, list}]),
                mochiweb_headers:enter("Cookie", CookieHeader1, Headers)
            end,
        mochiweb_headers:to_list(Headers1);
    _ ->
        mochiweb_headers:to_list(Headers)
    end.

%% @spec make_url({Domain::string(), Port::int()}, Uri::string()) -> string()
make_url({Domain, Port}, Uri) ->
    "http://" ++ Domain ++ ":" ++ ?i2l(Port) ++ Uri.


%% Internal API

add_rev_to_raw_path(ReqParams, RawPath, ETag) ->
    case ETag of
    undefined ->
        RawPath;
    _ ->
        case string:chr(ETag, $-) of
        0 -> % view ETags don't have a "-" character
            RawPath; % a view ETag can't be used as revision number
        _ ->
            ETag1 = string:strip(ETag, both, $"),
            case ReqParams of
            [] -> RawPath ++ "?rev=" ++ ETag1;
            _ -> RawPath ++ "&rev=" ++ ETag1
            end
        end
    end.

get_option(Option, Options) ->
    {proplists:get_value(Option, Options), proplists:delete(Option, Options)}.

%% @spec handle_read_request(Request, Addr::address(), RawPath::string(), HeaderList::header_list(), Method::atom()) -> ok
handle_read_request(Req, Addr, RawPath, HeaderList, Method) ->
    HeaderList1 = case couchdbcp:get_app_env(this_couch) of
                  Addr -> HeaderList;
                  _ -> lists:keystore("X-CouchDBCP-Consistency", 1, HeaderList, {"X-CouchDBCP-Consistency", eventual})
                  end,
    HeaderList2 = lists:keydelete('Host', 1, HeaderList1),
    Url = make_url(Addr, RawPath),
    case ibrowse:send_req(Url, HeaderList2, Method, [], [{stream_to, {self(), once}}|?IBROWSE_OPTIONS]) of
    {error, Reason} ->
        error_logger:info_msg("~p: ~p - ~p~n", [Method, Url, Reason]),
        {error, Reason};
    {ibrowse_req_id, ReqId} ->
        receive
            {ibrowse_async_headers, ReqId, ResCode, ResHeaderList} ->
                ResCode1 = ?l2i(ResCode),
                ResHeaderList1 = replace_location_header(ResHeaderList, Req),
                Res = Req:start_response({ResCode1, ResHeaderList1}),
                ibrowse:stream_next(ReqId),
                case lists:keyfind("Transfer-Encoding", 1, ResHeaderList1) of
                {_, "chunked"} -> stream_response_body(Res, ReqId);
                _ -> send_response_body(Req, ReqId, <<>>)
                end
        after ?TIMEOUT ->
            {error, timeout}
        end
    end.

%% @spec handle_write_request(Request, Addr::address(), RawPath::string(), header_list(), Method::method(), Body::binary()) -> ok
handle_write_request(Req, Addr, RawPath, HeaderList, Method, Body) ->
    HeaderList1 =
        case couchdbcp:get_app_env(this_couch) of
        Addr -> HeaderList;
        _ -> lists:keystore("X-CouchDBCP-Write", 1, HeaderList, {"X-CouchDBCP-Write", true})
        end,
    HeaderList2 = lists:keydelete('Host', 1, HeaderList1),
    % KLUDGE: This is an ibrowse issue; ibrowse adds a "content-length" header field--so prevent that it occurs twice.
    HeaderList3 = lists:keydelete('Content-Length', 1, HeaderList2),
    Url = make_url(Addr, RawPath),
    case ibrowse:send_req(Url, HeaderList3, Method, Body, [{stream_to, {self(), once}}|?IBROWSE_OPTIONS]) of
    {error, Reason} ->
        error_logger:info_msg("~p: ~p - ~p~n", [Method, Url, Reason]),
        {error, Reason};
    {ibrowse_req_id, ReqId} ->
        receive
            {ibrowse_async_headers, ReqId, ResCode, ResHeaderList} ->
                ResCode1 = ?l2i(ResCode),
                ResHeaderList1 = replace_location_header(ResHeaderList, Req),
                Res = Req:start_response({ResCode1, ResHeaderList1}),
                ibrowse:stream_next(ReqId),
                case lists:keyfind("Transfer-Encoding", 1, ResHeaderList1) of
                {_, "chunked"} -> stream_response_body(Res, ReqId);
                _ -> send_response_body(Req, ReqId, <<>>)
                end
        after ?TIMEOUT ->
            {error, timeout}
        end
    end.

stream_response_body(Res, ReqId) ->
    receive
        {ibrowse_async_response, ReqId, {error, Reason}} ->
            {error, Reason};
        {ibrowse_async_response, ReqId, Data} ->
            Res:write_chunk(Data),
            ibrowse:stream_next(ReqId),
            stream_response_body(Res, ReqId);
        {ibrowse_async_response_end, ReqId} ->
            ok
    % Here, we don't have an `after' clause, since it may take arbitrarily
    % long until another chunk is sent (just think of continuous replication).
    end.

send_response_body(Req, ReqId, Data) ->
    receive
        {ibrowse_async_response, ReqId, {error, Reason}} ->
            {error, Reason};
        {ibrowse_async_response, ReqId, Data1} ->
            ibrowse:stream_next(ReqId),
            send_response_body(Req, ReqId, <<Data/binary,Data1/binary>>);
        {ibrowse_async_response_end, ReqId} ->
            Req:send(Data)
    after ?TIMEOUT ->
        {error, timeout}
    end.

%% @spec send_header(Req, Code::200|304, Header::binary()|header_list()) -> ok
send_header(Req, Code, Header) ->
    HeaderBin = case is_binary(Header) of
                true -> Header;
                false -> header_list_to_binary(Header)
                end,
    case Code of
    200 -> Req:send(<<"HTTP/1.1 200 OK\r\n",HeaderBin/binary>>);
    304 -> Req:send(<<"HTTP/1.1 304 Not Modified\r\n",HeaderBin/binary>>)
    end.

%% @spec header_list_to_binary(HeaderList::header_list()) -> binary()
header_list_to_binary(HeaderList) ->
    B = ?l2b(lists:foldl(
                 fun({K, V}, Acc) ->
                     lists:concat([Acc, K, ": ", V, "\r\n"])
                 end, [], HeaderList)),
    <<B/binary,"\r\n">>.

%% @spec replace_location_header(HeaderList::header_list(), Request) -> header_list()
replace_location_header(HeaderList, Request) ->
    Host = Request:get_header_value("Host"),
    Forwarded_Host = Request:get_header_value("X-Forwarded-Host"),
    case lists:keyfind("Location", 1, HeaderList) of
    {_, Location} ->
        % preserve original DNS name
        Host1 = case Forwarded_Host of
                undefined -> Host;
                _ -> Forwarded_Host
                end,
        Location1 = re:replace(Location, "([hpst]+)://[^/]+(.*)",
            "\\1://" ++ Host1 ++ "\\2", [{return, list}]),
        lists:keystore("Location", 1, HeaderList, {"Location", Location1});
    false ->
        HeaderList
    end.

rpc(Pid, Request) ->
    Pid ! {self(), Request},
    RealPid = case is_atom(Pid) of
              true -> whereis(Pid);
              false -> Pid
              end,
    receive
        {RealPid, Res} ->
            Res
    end.
