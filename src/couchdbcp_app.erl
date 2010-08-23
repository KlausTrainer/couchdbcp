%%% This file is part of the CouchDBCP package and is released under the
%%% Tumbolia Public License. See LICENSE for more details.
%%%
%%% @author Klaus Trainer <klaus.trainer@web.de>

%%% @doc Callbacks for the CouchDBCP application.

-module(couchdbcp_app).
-author('Klaus Trainer <klaus.trainer@web.de>').

-include("couchdbcp.hrl").

-behaviour(application).
%% user interface
-export([start/2, stop/1, read_config/0, read_config/1]).


%% @spec start(_Type, _StartArgs) -> ServerRet
%% @doc application start callback for couchdbcp.
start(_Type, _StartArgs) ->
    case couchdbcp:get_app_env(no_config) of
    true -> nop; % promising to set all env variables some other way
    _ -> read_config()
    end,
    CookieTimeout = couchdbcp:get_app_env(cookie_timeout, 600000),
    {ok, Cache} = term_cache:start_link([{size, 4000000000},
        {ttl, CookieTimeout}, {name, cookie_store}]),
    set_erlenv([{cookie_store, Cache}]),
    register(replication_status_store, spawn(fun couchdbcp:replication_status_store/0)),
    start_peer_notifiers(),
    register(couchdbcp_app, self()),
    erlang:set_cookie(node(), couchdbcp:get_app_env(couchdbcp_cookie)),
    couchdbcp_deps:ensure(),
    couchdbcp_sup:start_link().

%% @spec stop(_State) -> ServerRet
%% @doc application stop callback for couchdbcp.
stop(_State) ->
    ok.

%% @spec read_config() -> ok | {error, Reason}
%% @doc Read the couchdbcp erlenv configuration file and set environment variables.
read_config() ->
    read_config(couchdbcp:get_app_env(configpath)).

%% @spec read_config(list()) -> ok | {error, Reason}
%% @doc Read the couchdbcp erlenv configuration file with filename ConfigPath
%%      and set environment variables.
read_config(ConfigPath) ->
    ConfigPairs = 
        case file:consult(ConfigPath) of
        {ok, Terms} ->
            Terms;
        {error, Reason} ->
            error_logger:warning_msg("Failed to read configuration from: ~p (~p)~n", [ConfigPath, Reason]),
            []
	    end,
    set_erlenv(ConfigPairs),
    {ok, [[NodeName]]} = init:get_argument(name),
    Nodes = couchdbcp:get_app_env(couchdbcp_nodes),
    case lists:keyfind(NodeName, 2, Nodes) of
    false ->
        Reason1 = "Stopping because there is no corresponding entry to this node name in the configuration file.",
        error_logger:warning_msg("~p~n", [Reason1]),
        stop(Reason1),
        {error, Reason1};
    ThisNodeInfo ->
        Proxies = lists:map(
                      fun(#node_info{proxy_name=N, proxy_port=P}) ->
                          {string:substr(N, string:chr(N, $@) + 1), P}
                      end, Nodes),
        set_erlenv([{proxies, Proxies}]),
        Couches = lists:map(
                      fun(#node_info{couch_name=N, couch_port=P}) ->
                          {string:substr(N, string:chr(N, $@) + 1), P}
                      end, Nodes),
        [] = lists:foldl(
                 fun(P, C) ->
                     [H|T] = C,
                     set_erlenv([{{couch_by_proxy, P}, H}]),
                     T
                 end, Couches, Proxies),
        #node_info{proxy_port=ProxyPort} = ThisNodeInfo,
        ProxyDomain = string:substr(NodeName, string:chr(NodeName, $@) + 1),
        set_erlenv([{this_proxy, {ProxyDomain, ProxyPort}}]),
        #node_info{couch_name=CouchName, couch_port=CouchPort} = ThisNodeInfo,
        CouchDomain = string:substr(CouchName, string:chr(CouchName, $@) + 1),
        set_erlenv([{this_couch, {CouchDomain, CouchPort}}]),
        ok
    end.


%% Internal API

start_peer_notifiers() ->
    lists:foreach(
        fun(Proxy) ->
            Pid = spawn(fun() ->
                            couchdbcp:peer_notifier(Proxy, queue:new())
                        end),
            set_erlenv([{{peer_notifier, Proxy}, Pid}])
        end, couchdbcp:get_app_env(proxies)).

set_erlenv([]) ->
    ok;
set_erlenv([{K, V}|T]) ->
    application:set_env(couchdbcp, K, V),
    error_logger:info_msg("set env variable ~p:~p~n",[K,V]),
    set_erlenv(T).
