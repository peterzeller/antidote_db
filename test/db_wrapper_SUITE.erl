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
-export([all/0]).
-export([test1/1, test2/1, test3/1]).

all() -> [test1, test2, test3].

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
    ok = antidote_db:put_op(Db, keyB, vectorclock:from_list([{dc2,50}]), log1),
    Records = antidote_db:get_ops(Db, c, vectorclock:from_list([{dc2,1}]), vectorclock:from_list([{dc2,100}])),
    ?assertEqual([log1], lists:sort(Records)),
    true
  end).


test3(_Config) ->
  withFreshDb(fun(Db) -> 
    ok = antidote_db:put_op(Db, keyB, vectorclock:from_list([]), log1),
    Records = antidote_db:get_ops(Db, keyB, vectorclock:from_list([]), vectorclock:from_list([{dc1,100}, {dc2,300}])),
    ?assertEqual([log1], lists:sort(Records)),
    true
  end).

