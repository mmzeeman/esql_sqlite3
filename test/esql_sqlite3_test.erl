%%
%%
%%

-module(esql_sqlite3_test).

-include("../src/edbc.hrl").

-include_lib("eunit/include/eunit.hrl").

open_single_database_test() ->
    {ok, C} = esql:open(esql_sqlite3, [":memory:"]),
    ok = esql:close(C),
    ok.

open_commit_close_test() ->
    {ok, C} = esql:open(esql_sqlite3, [":memory:"]),
    ok = esql:commit(C),
    ok = esql:close(C),
    ok.

open_rollback_close_test() ->
    {ok, C} = esql:open(esql_sqlite3, [":memory:"]),
    ok = esql:rollback(C),
    ok = esql:close(C),
    ok.

sql_syntax_error_test() ->
    {ok, C} = esql:open(esql_sqlite3, [":memory:"]),
    {error, esql_sqlite3, Msg} = esql:run("dit is geen sql", C),
    {sqlite3_error, "near \"dit\": syntax error"} = Msg,
    ok.

%%
%%
tables_test() ->
    {ok, C} = esql:open(esql_sqlite3, [":memory:"]),
    [] = esql:tables(C),
    ok = esql:run("create table table1(first_column char(50), second_column char(10));", C),
    [table1] = esql:tables(C),
    ok = esql:run("create table table2(first_column char(50), second_column char(10));", C),
    [table1, table2] = esql:tables(C),
    ok = esql:run("create table table3(first_column char(50), second_column char(10));", C),
    [table1, table2, table3] = esql:tables(C),
    ok = esql:run("drop table table2;", C),
    [table1, table3] = esql:tables(C),
    ok = esql:run("drop table table1;", C),
    [table3] = esql:tables(C),
    ok = esql:run("drop table table3;", C),
    [] = esql:tables(C), 
    ok.
    
%%
%%
describe_table_test() ->
    {ok, C} = esql:open(esql_sqlite3, [":memory:"]),
    [] = esql:tables(C),
    ok = esql:run("create table table1(first_column char(50) not null, 
       second_column char(10), 
       third_column INTEGER default 10,
       CONSTRAINT pk_first_column PRIMARY KEY (first_column));", C),
    [table1] = esql:tables(C),
    [Col1, Col2, Col3] = esql:describe_table(table1, C),
    first_column = Col1#esql_column_info.name,
    second_column = Col2#esql_column_info.name,
    third_column = Col3#esql_column_info.name,
    ok.

%%
%%
column_names_test() ->
    {ok, C} = esql:open(esql_sqlite3, [":memory:"]),
    [] = esql:tables(C),
    ok = esql:run("create table table1(first_column CHAR(50) not null, 
       second_column CHAR(10), 
       third_column INTEGER default 10,
       CONSTRAINT pk_first_column PRIMARY KEY (first_column));", C),
    [table1] = esql:tables(C),
    [first_column, second_column, third_column] = esql:column_names(table1, C).

%%
%%
execute_test() ->
    {ok, C} = esql:open(esql_sqlite3, [":memory:"]),
    ok = esql:run("create table table1(first_column char(50) not null, 
       second_column char(10), 
       third_column INTEGER default 10,
       CONSTRAINT pk_first_column PRIMARY KEY (first_column));", C),

    ok = esql:run("insert into table1 values(?, ?, ?);", ["hello", "world", 1], C),
    
    {ok, 
     {first_column, second_column, third_column}, 
     [{<<"hello">>, <<"world">>, 1}]} = esql:execute("select * from table1", C),

    {ok, 
     {first_column}, 
     [{<<"hello">>}]} = esql:execute("select t.first_column from table1 t", C),
    
    ok.
    
