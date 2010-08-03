#!/bin/sh
cd `dirname $0`
if [ -z "$1" ]; then
    echo "Cannot start: no node name specified!";
elif [ -z "$2" ]; then
    echo "Cannot start: no configuration file specified!";
else
	export NODENAME=$1
	export HEART_COMMAND="$(erl -name $1 -noshell -pa ebin -eval "error_logger:tty(false), couchdbcp_app:read_config(\"$2\"), io:format(\"~s~n\",[couchdbcp:get_app_env(couchdbcp_heart_command)])" -run init stop)"
    exec erl -connect_all false -pa $PWD/deps/*/ebin -pa $PWD/ebin -name $1 -s reloader -run couchdbcp start $2
fi
