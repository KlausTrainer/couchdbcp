%%% This file is part of the CouchDBCP package and is released under the
%%% Tumbolia Public License. See LICENSE for more details.
%%%
%%% @author Klaus Trainer <klaus.trainer@web.de>

%%% @doc CouchDBCP.

-module(couchdbcp).
-author('Klaus Trainer <klaus.trainer@web.de>').

-include("couchdbcp.hrl").

%% user interface
-export([start/0, start/1, stop/0]).
%% intermodule exports
-export([get_app_env/1, get_app_env/2, cookie_store/0, header_cache/0, check_read_quorum/3, notify_replication_success_to_peers/1, peer_notifier/2, replication_status_store/0, write/6]).

-define(IBROWSE_OPTIONS, [{response_format, binary}, {connect_timeout, 5000}, {inactivity_timeout, infinity}]).
-define(READ_TIMEOUT, 6000).
-define(WRITE_TIMEOUT, 8000).
-define(RANDOM_TIMEOUT, random:uniform(20000) + 10000).

%% @type method() = copy | delete | get | head | post | put.

        
%% @spec start([ConfigPath::list()]) -> ok
%% @doc Start couchdbcp.
%%      ``ConfigPath'' specifies the location of the couchdbcp configuration file.
start([ConfigPath]) ->
    application:set_env(couchdbcp, configpath, ConfigPath),
    start().

%% @spec start() -> ok
%% @doc Start the couchdbcp server.
start() ->
    couchdbcp_deps:ensure(),
    ensure_started(sasl),
    ensure_started(crypto),
    ensure_started(ibrowse),
    application:start(couchdbcp).

%% @spec stop() -> ok
%% @doc Stop the couchdbcp application and the calling process.
stop() ->
    stop("couchdbcp stop requested").

%% @spec stop(string()) -> ok
%% @doc Stop the couchdbcp server.
stop(Reason) ->
    error_logger:info_msg(io_lib:format("~p~n",[Reason])),
    Res = application:stop(couchdbcp),
    application:stop(ibrowse),
    application:stop(crypto),
    application:stop(sasl),
    Res.

%% @spec get_app_env(atom()) -> term()
%% @doc The official way to get the values set in couchdbcp's configuration
%%      file. Will return ``undefined'' if that option is unset.
get_app_env(Opt) ->
    get_app_env(Opt, undefined).

%% @spec get_app_env(atom(), term()) -> term()
%% @doc The official way to get the values set in couchdbcp's configuration
%%      file. Will return ``Default'' if that option is unset.
get_app_env(Opt, Default) ->
    case application:get_env(couchdbcp, Opt) of
    {ok, Val} ->
        Val;
    _ ->
        case init:get_argument(Opt) of
        {ok, [[Val|_]]} -> Val;
        error -> Default
        end
    end.

%% @spec header_cache() -> none()
%% @doc Caches document metadata, i.e. HTTP headers with ETag: ``{Header, ETag}''
header_cache() ->
    receive
        {From, {get, RawPath}} ->
            case get(?l2b(RawPath)) of
            undefined -> From ! {self(), undefined};
            {HeaderBin, ETagBin} -> From ! {self(), {HeaderBin, ?b2l(ETagBin)}}
            end,
            header_cache();
        {put, {RawPath, HeaderList, ETag}} ->
            put(?l2b(RawPath), {couchdbcp_web:header_list_to_binary(HeaderList), ?l2b(ETag)}),
            header_cache();
        {erase, RawPath} ->
            erase(?l2b(RawPath)),
            header_cache()
    end.

%% @spec cookie_store() -> none()
%% @doc Stores the peer's cookies, so we can authenticate to them.
cookie_store() ->
    receive
        {From, {get, KeyCookie}} ->
            case get(?l2b(KeyCookie)) of
            undefined -> From ! {self(), undefined};
            ValueCookie -> From ! {self(), ?b2l(ValueCookie)}
            end,
            cookie_store();
        {put, {KeyCookie, ValueCookie}} ->
            put(?l2b(KeyCookie), ?l2b(ValueCookie)),
            cookie_store();
        {erase, Cookie} ->
            erase(?l2b(Cookie)),
            cookie_store()
    end.

%% @spec replication_status_store() -> none()
%% @doc Stores databases that are replicated. For a database that is replicated, a key with the database name together with the value ``true'' is stored in the process dictionary.
replication_status_store() ->
    receive
        {From, {get, DB}} ->
            From ! {self(), get(DB)},
            replication_status_store();
        {put, DB} ->
            put(DB, true),
            replication_status_store();
        {erase, DB} ->
            erase(DB),
            replication_status_store()
    end.

%% @spec check_read_quorum(RawPath::string(), Cookie::string()|undefined, IfNoneMatch::string()|undefined) -> {ok, {RevNumber::binary(), Header::binary()|header_list(), Proxy::{Server::ip_address()|string(), Port::int()}}} | {error, Code::int()}
%% @doc Find a quorum for read requests.
check_read_quorum(RawPath, Cookie, IfNoneMatch) ->
    Proxies = get_app_env(proxies),
    Pids = lists:map(fun(Proxy) ->
                         Addr = case get_app_env(this_proxy) of
                                % if ``Proxy'' is ``this_proxy'',
                                % write directly to ``this_couch''
                                Proxy -> get_app_env(this_couch);
                                _ -> Proxy
                                end,
                         spawn(fun() -> reader(Addr, RawPath, Cookie, IfNoneMatch) end)
                     end, Proxies),
    MyPid = self(),
    Pid = spawn(fun() ->
                    read_response_processor(MyPid, length(Proxies), 0, [])
                end),
    lists:foreach(fun(Pid1) -> Pid1 ! {Pid, get_rev} end, Pids),
    receive
        {Pid, {error, Code}} ->
            {error, Code};
        {Pid, {ok, Result}} ->
            {ok, Result}
    end.

%% @spec write(Consistency::atomic|eventual, Method::method(), RawPath::string(), Cookie::string(), Headers::headers(), Body::binary()) -> {ok, {ResCode::int(), ResHeaderList::header_list(), ResBody::binary()}} | {error, Code}
%% @doc Depending on the value of ``Consistency'', this function starts a write
%%      operation with either eventual or atomic data consistency guaranteed.
write(Consistency, Method, RawPath, Cookie, Headers, Body) ->
    Proxies = get_app_env(proxies),
    Pids = lists:foldl(
               fun(Proxy, Pids) ->
                   Addr = case get_app_env(this_proxy) of
                          Proxy -> get_app_env(this_couch);
                          _ -> Proxy
                          end,
                   case RawPath of
                   "/_replicate" ->
                       Host = mochiweb_headers:get_value("Host", Headers),
                       {Domain, Port} = get_app_env({couch_by_proxy, Proxy}),
                       Addr1 = ?l2b([?l2b(Domain), <<":">>, ?l2b(?i2l(Port))]),
                       Body1 = re:replace(Body, ?l2b(Host), Addr1, [{return, binary}]);
                   _ -> 
                       Body1 = Body
                   end,
                   [spawn(fun() -> writer(Addr, Method, RawPath, Cookie, Headers, Body1) end)|Pids]
               end, [], Proxies),
    MyPid = self(),
    Pid = spawn(fun() ->
                    case Consistency of
                    atomic -> atomic_write_response_processor(MyPid, Method, RawPath, Cookie, Headers, length(Proxies), 0, []);
                    eventual -> eventual_write_response_processor(MyPid, Method, RawPath, Cookie, Headers, length(Proxies), 0, [])
                    end
                end),
    lists:foreach(fun(Pid1) ->
                      Pid1 ! {Pid, write}
                  end, Pids),
    receive
        {Pid, {error, Code}} ->
            {error, Code};
        {Pid, {ok, Result}} ->
            {ok, Result}
    end.

%% @spec peer_notifier(Proxy::address(), queue()) -> none()
%% @doc Periodically tries to notify a peer (``Proxy'') about missed updates.
%%      This will be retried until the peer is live again and until it has
%%      received the notification about missing updates.
peer_notifier(Proxy, Q) ->
    case get(seeded) of
    true ->
        ok;
    undefined -> % init random number generator
        random:seed(now()),
        put(seeded, true)
    end,
    case queue:is_empty(Q) of
    true ->
        Q2 = Q,
        Timeout = infinity;
    false ->
        {{value, DB}, Q1} = queue:out(Q),
        case get(DB) of
        undefined ->
            Q2 = Q1,
            Timeout = 0;
        true ->
            Url = couchdbcp_web:make_url(Proxy, "/" ++ DB),
            {Domain, Port} = get_app_env(this_couch),
            HeaderList = [{'X-CouchDBCP-Replicate', Domain ++ ":" ++ ?i2l(Port)}],
            case ibrowse:send_req(Url, HeaderList, post, [], ?IBROWSE_OPTIONS) of
            {error, _Reason} ->
                Q2 = Q,
                Timeout = ?RANDOM_TIMEOUT;
            {ok, ResCode, _ResHeaderList, ResBody} ->
                error_logger:info_msg("Told ~p to replicate ~p - response code ~p.~nResponse Body: ~p~n", [Proxy, DB, ?l2i(ResCode), ResBody]),
                erase(DB),
                Q2 = Q1,
                Timeout = 0
            end
        end
    end,
    receive
        {tell, DB1} ->
            case get(DB1) of
            undefined ->
                Q3 = queue:in(DB1, Q2),
                put(DB1, true);
            _ ->
                Q3 = Q2
            end,
            peer_notifier(Proxy, Q3);
        {has_replicated, DB1} ->
            erase(DB1),
            peer_notifier(Proxy, Q2)
    after Timeout ->
        peer_notifier(Proxy, Q2)
    end.

%% @spec notify_replication_success_to_peers(DB::string()) -> void()
notify_replication_success_to_peers(DB) ->
    ThisProxy = get_app_env(this_proxy),
    Proxies = lists:filter(fun(E) -> E =/= ThisProxy end, get_app_env(proxies)),    lists:foreach(
        fun(Proxy) ->
            Url = couchdbcp_web:make_url(Proxy, "/" ++ DB),
            {Server, Port} = ThisProxy,
            HeaderList = [{'X-CouchDBCP-Replicated', Server ++ ":" ++ ?i2l(Port)}],
            case ibrowse:send_req(Url, HeaderList, post, [], ?IBROWSE_OPTIONS) of
            {error, Reason} ->
                error_logger:info_msg("X-CouchDBCP-Replicated: notify ~p that I have replicated ~p - ~p~n", [Proxy, DB, Reason]);
            {ok, _ResCode, _ResHeaderList, _ResBody1} ->
                ok
            end
        end, Proxies).


%% Internal API

%% @spec tell_peers(ProxyList::[address()], DeadProxy::address(), DB::string) -> void()
%% @doc Tell all live peers about an update of ``DB'' that peer ``DeadProxy'' is missing.
tell_peers(ProxyList, DeadProxy, DB) ->
    lists:foreach(
        fun(Proxy) ->
            Url = couchdbcp_web:make_url(Proxy, "/" ++ DB),
            {Server, Port} = DeadProxy,
            HeaderList1 = [{'X-CouchDBCP-Tell', Server ++ ":" ++ ?i2l(Port)}],
            case ibrowse:send_req(Url, HeaderList1, post, [], ?IBROWSE_OPTIONS) of
            {error, Reason} ->
                error_logger:warning_msg("X-CouchDBCP-Tell ~p about ~p missing an update in ~p - ~p~n", [Proxy, DeadProxy, DB, Reason]);
            {ok, _ResCode, _ResHeaderList1, _ResBody1} ->
                ok
            end
        end, ProxyList).

%% @spec reader(Addr::address(), RawPath::string(), Cookie::string()|undefined, IfNoneMatch::string()|undefined) -> none()
reader(Addr, RawPath, Cookie, IfNoneMatch) ->
    ThisCouch = get_app_env(this_couch),
    receive
        {From, get_rev} ->
            Cached = 
                case Addr of
                ThisCouch -> % try to retrieve it from the cache
                    case rpc(header_cache, {get, RawPath}) of
                    undefined ->
                        false;
                    {HeaderBin, ETag} ->
                        Code = case ETag of
                               IfNoneMatch -> 304;
                               _ -> 200
                               end,
                        From ! {rev_info, Code, ETag, HeaderBin, Addr},
                        true
                    end;
                _ ->
                    false
                end,
            case Cached of
            true ->
                ok;
            false ->
                Url = couchdbcp_web:make_url(Addr, RawPath),
                HeaderList =
                    case Addr of
                    ThisCouch -> [];
                    _ -> [{'X-CouchDBCP-Consistency', eventual}]
                    end,
                HeaderList1 =
                    case Cookie of
                    undefined -> HeaderList;
                    _ -> [{'Cookie', "AuthSession=" ++ Cookie}|HeaderList]
                    end,
                HeaderList2 =
                    case IfNoneMatch of
                    undefined -> HeaderList1;
                    _ -> [{'If-None-Match', IfNoneMatch}|HeaderList1]
                    end,
                case ibrowse:send_req(Url, HeaderList2, head, [], ?IBROWSE_OPTIONS) of
                {error, Reason} ->
                    error_logger:info_msg("head: ~p - ~p~n", [Url, Reason]);
                {ok, ResCode, ResHeaderList, _} ->
                    ETag1 = case lists:keyfind("Etag", 1, ResHeaderList) of
                            false -> undefined;
                            {_, ETag2} -> ETag2
                            end,
                    case Addr of
                    ThisCouch when ETag1 =/= undefined andalso (ResCode =:= 200 orelse ResCode =:= 304) ->
                        {_DB, DocName} = couchdbcp_web:get_db_and_doc_name(RawPath),
                        case DocName of
                        "_all_docs" -> % don't cache
                            ok;
                        _ ->
                            header_cache ! {put, {RawPath, ResHeaderList, ETag1}}
                        end;
                    _ ->
                        ok
                    end,
                    From ! {rev_info, ?l2i(ResCode), ETag1, ResHeaderList, Addr}
                end
            end
    end.

%% @spec writer(Addr::address(), Method::method(), RawPath::string(), Cookie::string(), Headers::headers(), Body::binary()) -> ok | none()
writer(Addr, Method, RawPath, Cookie, Headers, Body) ->
    receive
        {From, write} ->
            Url = couchdbcp_web:make_url(Addr, RawPath),
            Headers1 = mochiweb_headers:enter('X-CouchDBCP-Write', "true", Headers),
            % KLUDGE: This is an ibrowse issue; ibrowse adds a "content-length" header field--so prevent that it occurs twice.
            Headers2 = mochiweb_headers:delete_any('Content-Length', Headers1),
            Headers3 = mochiweb_headers:delete_any('Host', Headers2),
            case ibrowse:send_req(Url, couchdbcp_web:make_header_list(Headers3, Cookie, Addr), Method, Body, ?IBROWSE_OPTIONS) of
            {error, Reason} ->
                error_logger:info_msg("~p: ~p - ~p~n", [Method, Url, Reason]);
            {ok, ResCode, ResHeaderList, ResBody} ->
                From ! {write_success, {?l2i(ResCode), ResHeaderList, ResBody}, Addr}
            end
    end.

read_response_processor(Pid, NumNodes, NumResps, Resps) when NumResps =:= NumNodes ->
    case find_read_quorum(lists:reverse(Resps), NumNodes) of
    {error, Code} -> Pid ! {self(), {error, Code}};
    {ok, Result} -> Pid ! {self(), {ok, Result}}
    end;
read_response_processor(Pid, NumNodes, NumResps, Resps) ->
    case NumResps > NumNodes div 2 of
    true ->
        case find_read_quorum(lists:reverse(Resps), NumNodes) of
        {error, _Code} ->
            Timeout = ?READ_TIMEOUT;
        {ok, Result} ->
            Timeout = 0,
            Pid ! {self(), {ok, Result}}
        end;
    false ->
        Timeout = ?READ_TIMEOUT
    end,
    receive
        {rev_info, ResCode, ETag, Header, Addr} ->
            read_response_processor(Pid, NumNodes, NumResps + 1, [{ResCode, ETag, Header, Addr}|Resps])
    after Timeout ->
        case Timeout of
        0 -> ok; % end process
        _ -> Pid ! {self(), {error, 503}}
        end
    end.

atomic_write_response_processor(Pid, _Method, _RawPath, Cookie, _ReqHeaders, NumNodes, NumResps, Results) when NumResps =:= NumNodes ->
    case get(quorum) of
    undefined ->
        case find_write_quorum(Results, NumNodes) of
        {error, Code} ->
            Pid ! {self(), {error, Code}};
        {ok, {Res, Addr}} ->
            {_ResCode, ResHeaderList, _ResBody} = Res,
            Pid ! {self(), {ok, Res}},
            send_cookies_to_peers(Addr, Cookie, ResHeaderList, Results)
        end;
    {{_ResCode, ResHeaderList, _ResBody}, Addr} ->
        send_cookies_to_peers(Addr, Cookie, ResHeaderList, Results)
    end;
atomic_write_response_processor(Pid, Method, RawPath, Cookie, ReqHeaders, NumNodes, NumResps, Results) ->
    Quorum = get(quorum),
    case NumResps > NumNodes div 2 of
    true when Quorum =:= undefined ->
        case find_write_quorum(Results, NumNodes) of
        {error, _Code} ->
            ok;
        {ok, {Res, Addr}} ->
            {_ResCode, ResHeaderList, _ResBody} = Res,
            put(quorum, {Res, Addr}),
            Pid ! {self(), {ok, Res}},
            send_cookies_to_peers(Addr, Cookie, ResHeaderList, Results)
        end;
    true ->
        {{_ResCode, ResHeaderList, _ResBody}, Addr} = Quorum,
        send_cookies_to_peers(Addr, Cookie, ResHeaderList, Results);
    _ ->
        ok
    end,
    receive
        {write_success, Res1, Addr1} ->
            atomic_write_response_processor(Pid, Method, RawPath, Cookie, ReqHeaders, NumNodes, NumResps + 1, [{Res1, Addr1}|Results])
    after ?WRITE_TIMEOUT ->
        case get(quorum) of
        undefined ->
            Pid ! {self(), {error, 503}};
        {{ResCode1, _ResHeaderList1, _ResBody1}, _Addr1} ->
            if
            ResCode1 =:= 200; ResCode1 =:= 201 ->
                Proxies = get_app_env(proxies),
                ThisCouch = get_app_env(this_couch),
                LivePeers = lists:foldl(fun(E, Acc) ->
                                            {_Res, Addr2} = E,
                                            case Addr2 of
                                            ThisCouch -> Acc;
                                            Addr3 -> [Addr3|Acc]
                                            end
                                        end, [], Results),
                lists:foreach(
                    fun(Addr2) ->
                        Addr3 = case get_app_env(this_proxy) of
                                Addr2 -> ThisCouch;
                                _ -> Addr2
                                end,
                        case lists:keymember(Addr3, 2, Results) of
                        true ->
                            ok;
                        false ->
                            {DB, _DocName} = couchdbcp_web:get_db_and_doc_name(RawPath),
                            case string:substr(DB, 1, 1) =:= "_" of
                            true ->
                                ok;
                            false ->
                                Pid1 = get_app_env({peer_notifier, Addr2}),
                                Pid1 ! {tell, DB},
                                tell_peers(LivePeers, Addr2, DB)
                            end
                        end
                    end, Proxies);
            true ->
                ok
            end
        end
    end.

eventual_write_response_processor(_Pid, _Method, _RawPath, Cookie, _ReqHeaders, NumNodes, NumResps, Results) when NumResps =:= NumNodes ->
    {{_ResCode, ResHeaderList, _ResBody}, Addr} = get(result),
    send_cookies_to_peers(Addr, Cookie, ResHeaderList, Results);
eventual_write_response_processor(Pid, Method, RawPath, Cookie, ReqHeaders, NumNodes, NumResps, Results) ->
    case NumResps of
    0 ->
        ok;
    1 ->
        [Result] = Results,
        put(result, Result),
        {Res, _Addr} = Result,
        Pid ! {self(), {ok, Res}};
    _ ->
        {Res, Addr} = get(result),
        {_ResCode, ResHeaderList, _ResBody} = Res,
        send_cookies_to_peers(Addr, Cookie, ResHeaderList, Results)
    end,
    receive
        {write_success, Res1, Addr1} ->
            eventual_write_response_processor(Pid, Method, RawPath, Cookie, ReqHeaders, NumNodes, NumResps + 1, [{Res1, Addr1}|Results])
    after ?WRITE_TIMEOUT ->
        case get(result) of
        undefined ->
            Pid ! {self(), {error, 503}};
        {Res1, _Addr1} ->
            {ResCode1, _ResHeaderList1, _ResBody1} = Res1,
            if
            ResCode1 =:= 200; ResCode1 =:= 201 ->
                Proxies = get_app_env(proxies),
                ThisCouch = get_app_env(this_couch),
                LivePeers = lists:foldl(fun(E, Acc) ->
                                            {_Res, Addr2} = E,
                                            case Addr2 of
                                            ThisCouch -> Acc;
                                            Addr3 -> [Addr3|Acc]
                                            end
                                        end, [], Results),
                lists:foreach(
                    fun(Addr2) ->
                        Addr3 = case get_app_env(this_proxy) of
                                Addr2 -> ThisCouch;
                                _ -> Addr2
                                end,
                        case lists:keymember(Addr3, 2, Results) of
                        true ->
                            ok;
                        false ->
                            {DB, _DocName} = couchdbcp_web:get_db_and_doc_name(RawPath),
                            case string:substr(DB, 1, 1) =:= "_" of
                            true ->
                                ok;
                            false ->
                                Pid1 = get_app_env({peer_notifier, Addr2}),
                                Pid1 ! {tell, DB},
                                tell_peers(LivePeers, Addr2, DB)
                            end
                        end
                    end, Proxies);
            true ->
                ok
            end
        end
    end.

send_cookies_to_peers(Addr, Cookie, ResHeaderList, Results) ->
    NewCookie = case lists:keyfind("Set-Cookie", 1, ResHeaderList) of
                false -> undefined;
                {_, CookieHeader} -> proplists:get_value("AuthSession", mochiweb_cookies:parse_cookie(CookieHeader))
                end,
    case NewCookie of
    undefined ->
        ok;
    _ ->
        ThisCouch = get_app_env(this_couch),
        lists:foreach(
            fun(Res) ->
                {{_ResCode, ResHeaderList1, _ResBody}, Addr1} = Res,
                HasCookie = get({has_cookie, Addr1}),
                case lists:keyfind("Set-Cookie", 1, ResHeaderList1) of
                {_, ResCookieHeader} when Addr1 =/= Addr andalso HasCookie =:= undefined ->
                    ResCookie = proplists:get_value("AuthSession", mochiweb_cookies:parse_cookie(ResCookieHeader)),
                    case Addr1 of
                    ThisCouch ->
                        case ResCookie of
                        [] when Cookie =/= undefined ->
                            cookie_store ! {erase, Cookie};
                        _ ->
                            cookie_store ! {put, {NewCookie, ResCookie}}
                        end;
                    Addr1 ->
                        Url = couchdbcp_web:make_url(Addr1, ""),
                        case ResCookie of
                        [] when Cookie =/= undefined ->
                            HeaderList = [{'X-CouchDBCP-Unset-Cookie', Cookie}],
                            Body = [];
                        _ ->
                            HeaderList = [{'X-CouchDBCP-Set-Cookie', "true"}],
                            Body = ?t2b({NewCookie, ResCookie})
                        end,
                        case ibrowse:send_req(Url, HeaderList, post, Body, ?IBROWSE_OPTIONS) of
                        {error, Reason} ->
                            error_logger:warning_msg("X-CouchDBCP-Set-Cookie: error with ~p - ~p~n", [Addr, Reason]);
                        {ok, _ResCode1, _ResHeaderList2, _ResBody1} ->
                            ok
                        end
                    end;
                _ ->
                    ok
                end,
                put({has_cookie, Addr1}, true)
            end, Results)
    end.

find_read_quorum(Replies, N) ->
    case lists:dropwhile(fun(E) ->
                             {ResCode, ETag, _Header, _Addr} = E,
                             case lists:filter(fun(E1) ->
                                                   {ResCode1, ETag1, _, _} = E1,
                                                   case ResCode1 of
                                                   ResCode when ETag1 =:= ETag ->
                                                       true;
                                                   _ ->
                                                       false
                                                   end
                                               end, Replies) of
                             L when length(L) =< N div 2 -> true;
                             _ -> false
                             end
                         end, Replies) of
    [] -> {error, 503};
    [H|_] -> {ok, H}
    end.

find_write_quorum(Replies, NumNodes) ->
    case lists:dropwhile(fun(E) ->
                             {{ResCode, _HeaderList, _Body}, _Addr} = E,
                             case lists:filter(fun(E1) ->
                                                   {{ResCode1, _, _}, _} = E1,
                                                   case ResCode1 of
                                                   ResCode ->
                                                       true;
                                                   _ ->
                                                       false
                                                   end
                                               end, Replies) of
                             L when length(L) =< NumNodes div 2 -> true;
                             _ -> false
                             end
                         end, Replies) of
    [] -> {error, 503};
    [{Res, Addr}|_] -> {ok, {Res, Addr}}
    end.

ensure_started(App) ->
    case application:start(App) of
    ok -> ok;
    {error, {already_started, App}} -> ok
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
