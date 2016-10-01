-module(prop_get_ops).


-define(PROPER_NO_TRANS, true).
-include_lib("proper/include/proper.hrl").

-include_lib("antidote_utils/include/antidote_utils.hrl").




-export([prop_get_ops/0, blub/0]).


blub() ->
  Ops = [{inc,dc2,1},{inc,dc3,1},{inc,dc2,1},{pull,dc2,dc3},{inc,dc3,1}],
  checkSpec(opsToClocks(Ops)).

prop_get_ops() ->
	?FORALL(Ops, generateOps(),
      checkSpec(opsToClocks(Ops))
    ).

checkSpec(Clocks) ->
%%  io:format("Clocks = ~p~n", [Clocks]),
  VClocks = [vectorclock:from_list(C) || C <- Clocks],
  db_wrapper_SUITE:withFreshDb(
    fun(Db) ->
      % insert all the operations
      {_, Entries} = lists:foldl(
        fun(Clock, {I, Entries}) ->
          Entry = {logEntry, I},
          ok = antidote_db:put_op(Db, key, Clock, Entry),
          {I + 1, [{Clock, Entry}|Entries]}
        end, {1, []}, VClocks),
      % generate all possible {From, To} pairs from the clocks
      ClockPairs = [{From,To} || From <- VClocks, To <- VClocks, vectorclock:le(From, To)],
      lists:all(fun({From, To}) -> checkGetOps(Db, Entries, From, To) end, ClockPairs)
    end).

checkGetOps(Db, Entries, From, To) ->
  Records = antidote_db:get_ops(Db, key, From, To),
  Expected = [Rec ||
    {Clock, Rec} <- Entries,
    not vectorclock:le(Clock, From),
    vectorclock:le(Clock, To)
    ],

  case lists:sort(Records) == lists:sort(Expected) of
    true -> true;
    false ->
      io:format("~n---- Start of testcase -------~n"),
      [io:format("ok = antidote_db:put_op(Db, key, vectorclock:from_list(~w), ~w),~n",
        [dict:to_list(C), E]) || {C,E} <- Entries],
      io:format("Records = antidote_db:get_ops(Db, key, vectorclock:from_list(~w), vectorclock:from_list(~w)),~n",
        [dict:to_list(From), dict:to_list(To)]),
      io:format("?assertEqual(~w, lists:sort(Records)),~n", [lists:sort(Expected)]),
      io:format("% returned ~w~n", [Records]),
      io:format("---- End of testcase -------~n"),
      false
  end .



generateClocks() ->
  ?LET(Ops, generateOps(), opsToClocks(Ops)).

opsToClocks(Ops) ->
  {Clocks, _} = execOps(Ops, {[], orddict:from_list([{R, [vectorclock:new()]} || R <- replicas()])}),
  lists:sort([dict:to_list(C) || C <- Clocks]).

execOps([], State) -> State;
execOps([{pull, SourceR, TargetR}|RemainingOps], {OpClocks, State}) ->
  SourceClocks = orddict:fetch(SourceR, State),
  TargetClock = lists:last(orddict:fetch(TargetR, State)),
  NewSourceClocks = [C || C <- SourceClocks, not vectorclock:le(C, TargetClock)],
  MergedClock =
    case NewSourceClocks of
      [] -> TargetClock;
      [C|_] ->
        vectorclock:max([C, TargetClock])
    end,
  NewState = orddict:append(TargetR, MergedClock, State),
  execOps(RemainingOps, {OpClocks, NewState});
execOps([{inc, R, Amount}|RemainingOps], {OpClocks, State}) ->
  RClock = lists:last(orddict:fetch(R, State)),
  NewClock = vectorclock:set_clock_of_dc(R, Amount + vectorclock:get_clock_of_dc(R, RClock), RClock),
  NewState = orddict:append(R, NewClock, State),
  execOps(RemainingOps, {[NewClock|OpClocks], NewState}).

generateOps() ->
  list(oneof([
    % pulls one operation from the first replica to the second
    {pull, replica(), replica()},
    % increment vector clock
    {inc, replica(), range(1,3)}
  ])).

replica() ->
  oneof(replicas()).

replicas() ->[dc1, dc2, dc3].





