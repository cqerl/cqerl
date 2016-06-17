-module(cqerl_error_m).
-behaviour(monad).
-export(['>>='/2, return/1, fail/1]).

'>>='({error, _Err} = Error, _Fun) -> Error;
'>>='({ok, Result},           Fun) -> Fun(Result);
'>>='(ok,                     Fun) -> Fun(ok).

return(X) -> {ok,    X}.
fail(X)   -> {error, X}.
