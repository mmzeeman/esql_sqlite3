%%
%% Exploring making a database driver for the generic gen_db interface
%%

-module(esql_sqlite3).

-include_lib("esql/include/esql.hrl").

-behaviour(esql).

%% esql behaviour callbacks
-export([
    open/1, 
    run/3,  
    execute/3, execute/4, 
    close/1, 
    start_transaction/1, 
    commit/1, 
    rollback/1,
    table_exists/2, 
    tables/1, 
    describe_table/2
]).

%% @doc Open a database connection
%%
open([DatabaseName]) ->
    {ok, C} = esqlite3:open(DatabaseName),
    {ok, C}.


%% @doc Close the connection
%%
close(Connection) ->
    ok = esqlite3:close(Connection).

%% @doc 
%%
run(Sql, [], Connection) ->
    case esqlite3:exec(Sql, Connection) of
        {error, Error} -> 
            {error, ?MODULE, Error};
        ok -> 
            ok
    end;
run(Sql, Args, Connection) ->
    case esqlite3:prepare(Sql, Connection) of
        {error, Error} ->
            {error, ?MODULE, Error};
        {ok, Stmt} ->
            case esqlite3:bind(Stmt, Args) of
                {error, Error} -> 
                    {error, ?MODULE, Error};
                ok ->
                    case esqlite3:fetchone(Stmt) of
                        {error, Error} -> 
                            {error, ?MODULE, Error};
                        _ -> ok
                    end
            end
    end.

%% @doc Execute a query and return the results
%%
execute(Sql, [], Connection) ->
    case esqlite3:prepare(Sql, Connection) of
        {error, Error} ->
            {error, ?MODULE, Error};
        {ok, Stmt} ->
            {ok, tuple_to_list(esqlite3:column_names(Stmt)), esqlite3:fetchall(Stmt)}
    end;
execute(Sql, Args, Connection) ->
    case esqlite3:prepare(Sql, Connection) of
        {error, Error} ->
            {error, ?MODULE, Error};
        {ok, Stmt} ->
            case esqlite3:bind(Stmt, Args) of
                ok ->
                    Names = esqlite3:column_names(Stmt),
                    Result = esqlite3:fetchall(Stmt),
                    {ok, tuple_to_list(Names), Result};
                {error, Error} ->
                    {error, ?MODULE, Error}
            end
    end.

%% Asynchronous execute.. send answers when the arrive...
execute(Sql, Args, Receiver, Connection) ->
    Pid = spawn(fun() -> 
        handle_async_execute(Sql, Args, Receiver, Connection) 
    end),
    {ok, Pid}.


%%
handle_async_execute(Sql, Args, Receiver, Connection) ->
    case esqlite3:prepare(Sql, Connection) of
        {error, Error} ->
            Receiver ! {error, ?MODULE, Error};
        {ok, Stmt} ->
            case bind_args(Stmt, Args) of
                {error, Error} ->
                    Receiver ! {error, ?MODULE, Error};
                ok ->
                    %% Send the column names.
                    Names = esqlite3:column_names(Stmt),
                    Receiver ! {self(), column_names, Names},
                    receive 
                        continue ->
                            send_rows(Stmt, Receiver);
                        stop ->
                            Receiver ! {self(), stopped}
                    after 
                        10000 ->
                            %% TIMEOUT
                            timeout
                    end
            end
    end.

send_rows(Stmt, Receiver) ->
    %% This one can be implemented directly in the low level driver.
    
    case esqlite3:step(Stmt) of
        '$busy' ->
            %% wait... or better with exponential backoff?
            timer:sleep(100),
            send_rows(Stmt, Receiver);
        '$done' ->
            Receiver ! {self(), done};
        {row, Row} ->
            Receiver ! {self(), row, Row},
            receive 
                continue -> 
                    send_rows(Stmt, Receiver);
                stop ->
                    Receiver ! {self(), stopped}
            after
                10000 ->
                    {self(), timeout}
            end
    end.

%%
bind_args(_Statement, []) ->
    ok;
bind_args(Statement, Args) ->
    esqlite3:bind(Statement, Args).

%%
start_transaction(Connection) ->
    run(<<"START TRANSACTION;">>, [], Connection).

%%
commit(Connection) ->
    run(<<"COMMIT;">>, [], Connection).

%%
rollback(Connection) ->
    run(<<"ROLLBACK;">>, [], Connection).

%% @doc return true iff the table exists.
table_exists(Name, Connection) ->
    case esqlite3:q(<<"SELECT count(type) FROM sqlite_master WHERE type='table' AND name=?;">>, 
                [Name], Connection) of
        [{1}] -> true;
        [{0}] -> false
    end.

%% @doc Return a list with all tables.
tables(Connection) ->
    esqlite3:map(fun({Name}) -> erlang:binary_to_atom(Name, utf8) end, 
                 <<"SELECT name FROM sqlite_master WHERE type='table' ORDER by name;">>, Connection).

%% @doc Return a descripion of the table.
describe_table(TableName, Connection) when is_atom(TableName) ->
    esqlite3:map(fun({_Cid, ColumnName, ColumnType, NotNull, Default, PrimaryKey}) -> 
                    #esql_column_info{name=erlang:binary_to_atom(ColumnName, utf8),
                                      type=ColumnType,
                                      default=Default,
                                      notnull=NotNull =/= 0,
                                      pk=PrimaryKey =/= 0}
                 end,
                 [<<"PRAGMA table_info('">>, erlang:atom_to_binary(TableName, utf8), <<"');">>], Connection).

