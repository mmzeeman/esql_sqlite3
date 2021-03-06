%%
%% Tests for sqlite3
%%

-module(esql_sqlite3_test).

-include_lib("esql/include/esql.hrl").
-include_lib("eunit/include/eunit.hrl").

open_single_database_test() ->
    {ok, C} = esql:open(esql_sqlite3, [":memory:"]),
    ok = esql:close(C).

open_commit_close_test() ->
    {ok, C} = esql:open(esql_sqlite3, [":memory:"]),
    ?assertMatch({error, esql_sqlite3, _Msg}, esql:commit(C)),  %% No transaction is started.
    ok = esql:close(C),
    ok.

open_rollback_close_test() ->
    {ok, C} = esql:open(esql_sqlite3, [":memory:"]),
    ?assertMatch({error, esql_sqlite3, _Msg}, esql:rollback(C)), %% No transaction is started.
    ok = esql:close(C),
    ok.

sql_syntax_error_test() ->
    {ok, C} = esql:open(esql_sqlite3, [":memory:"]),
    ?assertEqual({error, esql_sqlite3, {sqlite_error, "near \"dit\": syntax error"}}, 
        esql:run("dit is geen sql", C)),
    ok.

%%
%%
tables_test() ->
    {ok, C} = esql:open(esql_sqlite3, [":memory:"]),
    [] = esql:tables(C),
    ok = esql:run("create table table1(first_column char(50), second_column char(10));", C),
    ?assertEqual([table1], esql:tables(C)),
    ok = esql:run("create table table2(first_column char(50), second_column char(10));", C),
    ?assertEqual([table1, table2], esql:tables(C)),
    ok = esql:run("create table table3(first_column char(50), second_column char(10));", C),
    ?assertEqual([table1, table2, table3], esql:tables(C)),
    ok = esql:run("drop table table2;", C),
    ?assertEqual([table1, table3], esql:tables(C)),
    ok = esql:run("drop table table1;", C),
    ?assertEqual([table3], esql:tables(C)),
    ok = esql:run("drop table table3;", C),
    ?assertEqual([], esql:tables(C)), 
    ok.

%%
%%
table_exists_test() ->
    {ok, C} = esql:open(esql_sqlite3, [":memory:"]),
    false = esql:table_exists("table1", C),
    ok = esql:run("create table table1(first_column text);", C),
    true = esql:table_exists("table1", C),
    ok = esql:run("drop table table1;", C),
    false = esql:table_exists("table1", C),
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

    R1 = esql:execute("select * from table1", C),

    ?assertEqual(
    {ok, [first_column, second_column, third_column], 
     [{<<"hello">>, <<"world">>, 1}]}, R1),

    R2 = esql:execute("select t.first_column from table1 t", C),
    
    ?assertEqual({ok, [first_column], [{<<"hello">>}]}, R2),

    ok = esql:run("insert into table1 values(?, ?, ?);", [<<"spam">>, <<"eggs">>, 2], C),

    R3 = esql:execute("select * from table1", C),
    ?assertEqual({ok, [first_column, second_column, third_column], 
     [{<<"hello">>, <<"world">>, 1},
      {<<"spam">>, <<"eggs">>, 2}
     ]}, R3),

    R4 = esql:execute("select * from table1 where third_column=2", C),
    ?assertEqual({ok, [first_column, second_column, third_column], 
     [{<<"spam">>, <<"eggs">>, 2}
     ]}, R4),
    
    ok.
    
%%
%% Async execute test
async_simple_execute_test() ->
    {ok, C} = esql:open(esql_sqlite3, [":memory:"]),

    %% Create a test table
    ok = esql:run("create table table1(first_column char(50) not null, 
       second_column char(10), 
       third_column INTEGER default 10,
       CONSTRAINT pk_first_column PRIMARY KEY (first_column));", C),

    {ok, Ref} = esql:execute("select * from table1;", [], self(), C),

    %% First get the column names.
    {first_column, second_column, third_column} = receive 
        {Ref, column_names, Cols}-> 
           Ref ! continue,
           Cols
        end,
 
    %% And then the query result. In this case nothing...
    receive {Ref, done} -> ok end.

%%
%% Async execute test
async_execute_test() ->
    {ok, C} = esql:open(esql_sqlite3, [":memory:"]),

    %% Create a test table
    ok = esql:run("create table table1(first_column char(50) not null, 
       second_column char(10), 
       third_column INTEGER default 10,
       CONSTRAINT pk_first_column PRIMARY KEY (first_column));", C),

    Sql = "insert into table1 values(?, ?, ?);",

    ok = esql:run(Sql, [<<"spam">>, <<"eggs">>, 1], C),
    ok = esql:run(Sql, [<<"foo">>, <<"bar">>, 2], C),
    ok = esql:run(Sql, [<<"zoto">>, <<"magic">>, 3], C),

    {ok, Ref} = esql:execute("select * from table1 order by third_column;", [], self(), C),

    %% First get the column names.
    {column_names, {first_column, second_column, third_column}} = esql:step(Ref),

    %% Get the rows... 
    ?assertEqual({row, {<<"spam">>, <<"eggs">>, 1}}, esql:step(Ref)), 
    ?assertEqual({row, {<<"foo">>, <<"bar">>, 2}}, esql:step(Ref)),
    ?assertEqual({row, {<<"zoto">>, <<"magic">>, 3}}, esql:step(Ref)),

    %% Done..
    done = esql:step(Ref).

execute1_test() ->
    {ok, C} = esql:open(esql_sqlite3, [":memory:"]),

    ok = esql:run("create table table1(first_column char(50) not null, 
       second_column char(10), 
       third_column INTEGER default 10,
       CONSTRAINT pk_first_column PRIMARY KEY (first_column));", C),

    ?assertEqual({error, noresult}, esql:execute1(<<"select * from table1">>, C)),
    ok = esql:run("insert into table1 values('spam', 'eggs', 1)", C),
    ?assertEqual({ok,{<<"spam">>,<<"eggs">>,1}}, 
        esql:execute1(<<"select * from table1">>, C)), 
    ok = esql:run("insert into table1 values('green', 'tomatos', 2)", C),           

    ?assertEqual({ok,{<<"spam">>,<<"eggs">>,1}}, 
        esql:execute1(<<"SELECT * FROM table1 ORDER BY third_column;">>, C)), 
    ?assertEqual({ok,{<<"green">>,<<"tomatos">>,2}}, 
        esql:execute1(<<"SELECT * FROM table1 ORDER BY third_column DESC;">>, C)), 

    ?assertEqual({ok,{<<"spam">>,<<"eggs">>,1}}, 
        esql:execute1(<<"SELECT * FROM table1 ORDER BY third_column LIMIT 1;">>, C)),
    ?assertEqual({error, noresult}, 
        esql:execute1(<<"SELECT * FROM table1 ORDER BY third_column LIMIT 0;">>, C)),

    ok.

table_meta_test() ->
    {ok, C} = esql:open(esql_sqlite3, [":memory:"]),

    ?assertEqual(false, esql:table_exists("test", C)),

    ok = esql:run("create table table1(first_column char(50) not null, 
       second_column char(10), 
       third_column INTEGER default 10,
       CONSTRAINT pk_first_column PRIMARY KEY (first_column));", C),

    ?assertEqual(true, esql:table_exists(table1, C)),
    ?assertEqual(true, esql:table_exists("table1", C)), 
    ?assertEqual(true, esql:table_exists(<<"table1">>, C)),

    ok.

pragma_test() ->
    %% no pragma.
    ?assertMatch({ok, _C1}, esql:open(esql_sqlite3, [":memory:"])),

    %% one pragma.
    ?assertMatch({ok, _C2}, esql:open(esql_sqlite3, [":memory:", [
        {pragma, "case_sensitive_like = 1"}]])),

    %% more pragma's.
    ?assertMatch({ok, _C3}, esql:open(esql_sqlite3, [":memory:", [
        {pragma, "encoding = \"UTF-8\""}, {pragma, "case_sensitive_like=1"}]])),

    ok.

simple_pool_test() ->
    application:start(esql),
    {ok, _Pid} = esql_pool:create_pool(test_pool, 10, 
                                      [{driver, esql_sqlite3}, 
                                       {args, ["file:memdb1?mode=memory&cache=shared"]}]),

    ok = esql_pool:run("create table table1(first_column char(50) not null, 
       second_column char(10), 
       third_column INTEGER default 10,
                       CONSTRAINT pk_first_column PRIMARY KEY (first_column));", [], test_pool),

    Sql = "insert into table1 values(?, ?, ?);",

    ok = esql_pool:run(Sql, [<<"spam">>, <<"eggs">>, 1], test_pool),
    ok = esql_pool:run(Sql, [<<"foo">>, <<"bar">>, 2], test_pool),
    ok = esql_pool:run(Sql, [<<"zoto">>, <<"magic">>, 3], test_pool),

    {ok, Cols, Rows} = 
        esql_pool:execute("select * from table1 order by first_column;", [], test_pool),

    [first_column, second_column, third_column] = Cols,
    [{<<"foo">>, <<"bar">>, 2},
     {<<"spam">>, <<"eggs">>, 1},
     {<<"zoto">>, <<"magic">>, 3}] = Rows,
    
    esql_pool:delete_pool(test_pool),
    ok.

