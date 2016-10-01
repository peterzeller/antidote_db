%%%-------------------------------------------------------------------
%%% @author peter
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 18. Sep 2016 12:33
%%%-------------------------------------------------------------------
-module(db_wrapper_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("antidote_utils/include/antidote_utils.hrl").
-include_lib("eunit/include/eunit.hrl").
-export([all/0, withFreshDb/1]).
-export([test1/1, test2/1, test3/1, test4/1, test5/1, test6/1, test7/1, test8/1]).

all() -> [test1, test2, test3, test4, test5, test6, test7, test8].

withFreshDb(F) ->
  {ok, Db} = antidote_db:new("test_db", leveldb),
  try
    F(Db)
  after
    antidote_db:close_and_destroy(Db, "test_db")
  end.

test1(_Config) ->
  withFreshDb(fun(Db) -> 
    ok = antidote_db:put_op(Db, a, vectorclock:from_list([{a,1}]), log1),
    ok = antidote_db:put_op(Db, a, vectorclock:from_list([{b,1}]), log2),
    ok = antidote_db:put_op(Db, a, vectorclock:from_list([{a,1},{b,1}]), log3),

    Records = antidote_db:get_ops(Db,a, vectorclock:from_list([]), vectorclock:from_list([{a,1},{b,1}])),
    ?assertEqual([log1, log2, log3], lists:sort(Records)),
    true
  end).


test2(_Config) -> 
  withFreshDb(fun(Db) -> 
    ok = antidote_db:put_op(Db, b, vectorclock:from_list([{dc2,50}]), log1),
    Records = antidote_db:get_ops(Db, b, vectorclock:from_list([{dc2,1}]), vectorclock:from_list([{dc2,50}])),
    ?assertEqual([log1], lists:sort(Records)),
    true
  end).


test3(_Config) -> 
  withFreshDb(fun(Db) -> 
    ok = antidote_db:put_op(Db, c, vectorclock:from_list([{dc2,51}]), log1),
    Records = antidote_db:get_ops(Db, c, vectorclock:from_list([{dc2,50}]), vectorclock:from_list([{dc2,100}])),
    ?assertEqual([log1], lists:sort(Records)),
    true
  end).

test4(_Config) ->
  withFreshDb(fun(Db) -> 
    ok = antidote_db:put_op(Db, d, vectorclock:from_list([{dc1,1},{dc3,2}]), logEntry),
    Records = antidote_db:get_ops(Db, d, vectorclock:from_list([{dc1,2},{dc3,1}]), vectorclock:from_list([{dc1,3},{dc3,3}])),
    ?assertEqual([logEntry], lists:sort(Records)),
    true
  end).

test5(_Config) ->
  withFreshDb(fun(Db) -> 
    ok = antidote_db:put_op(Db, d, vectorclock:from_list([{dc1,7},{dc2,14},{dc3,14}]), logEntry),
    Records = antidote_db:get_ops(Db, d, vectorclock:from_list([{dc2,5},{dc3,7}]), vectorclock:from_list([{dc1,10},{dc2,11},{dc3,17}])),
    ?assertEqual([], lists:sort(Records)),
    true
  end).

test6(_Config) ->
  withFreshDb(fun(Db) -> 
    ok = antidote_db:put_op(Db, d, vectorclock:from_list([{dc1,9},{dc2,12},{dc3,1}]), logEntry1),
    ok = antidote_db:put_op(Db, d, vectorclock:from_list([{dc1,7},{dc2,2},{dc3,12}]), logEntry2),
    Records = antidote_db:get_ops(Db, d, vectorclock:from_list([{dc1,10},{dc2,17},{dc3,2}]), vectorclock:from_list([{dc1,12},{dc2,20},{dc3,18}])),
    ?assertEqual([logEntry2], lists:sort(Records)),
    true
  end).

test7(_Config) ->
  withFreshDb(fun(Db) -> 
    ok = antidote_db:put_op(Db, d, vectorclock:from_list([{dc1,1},{dc2,3},{dc3,3}]), logEntry1),
    ok = antidote_db:put_op(Db, d, vectorclock:from_list([{dc1,2},{dc2,0},{dc3,1}]), logEntry2),
    ok = antidote_db:put_op(Db, d, vectorclock:from_list([{dc1,2},{dc2,1},{dc3,1}]), logEntry3),
    Records = antidote_db:get_ops(Db, d, vectorclock:from_list([{dc1,3},{dc2,2},{dc3,2}]), vectorclock:from_list([{dc1,10},{dc2,10},{dc3,10}])),
    ?assertEqual([logEntry1], lists:sort(Records)),
    true
  end).

test8(_Config) ->
  withFreshDb(
    fun(Db) ->
      ok = antidote_db:put_op(Db, key, vectorclock:from_list([{dc3,1}]), {logEntry,4}),
      ok = antidote_db:put_op(Db, key, vectorclock:from_list([{dc2,3},{dc3,1}]), {logEntry,3}),
      ok = antidote_db:put_op(Db, key, vectorclock:from_list([{dc2,2}]), {logEntry,2}),
      ok = antidote_db:put_op(Db, key, vectorclock:from_list([{dc2,1}]), {logEntry,1}),
      Records = antidote_db:get_ops(Db, key, vectorclock:from_list([{dc2,2}]), vectorclock:from_list([{dc2,3},{dc3,1}])),
      ?assertEqual([{logEntry,3},{logEntry,4}], lists:sort(Records)),
      true
    end).


