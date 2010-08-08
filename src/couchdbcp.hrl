%% @type method() = copy | delete | get | head | post | put.
%% @type header_list() = [{Header::atom()|string(), Value::atom()|string()}]
%% @type address() = {Domain::ip_address()|string(), Port::int()}

-define(a2l(V), atom_to_list(V)).
-define(b2l(V), binary_to_list(V)).
-define(b2t(V), binary_to_term(V)).
-define(l2a(V), list_to_atom(V)).
-define(l2b(V), list_to_binary(V)).
-define(l2i(V), list_to_integer(V)).
-define(i2l(V), integer_to_list(V)).
-define(t2b(V), term_to_binary(V)).

-record(node_info, {
    proxy_name,
	proxy_port,
    couch_name,
    couch_port}).

-define(CPVSN, "0.2").
-define(QUIP, "CouchDBCP").
