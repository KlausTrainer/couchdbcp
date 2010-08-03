{application, couchdbcp,
 [{description, "CouchDBCP"},
  {vsn, "0.01"},
  {modules, [
    couchdbcp,
    couchdbcp_app,
    couchdbcp_sup,
    couchdbcp_web,
    couchdbcp_deps
  ]},
  {registered, [couchdbcp_sup]},
  {mod, {couchdbcp_app, []}},
  {applications, [crypto, ibrowse, kernel, sasl, stdlib]}
 ]}.
