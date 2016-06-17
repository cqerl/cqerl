-module(cql_parser).

-export([get_table/1]).

get_table(Statement) ->
    case re2:match(Statement, get_re()) of
        nomatch -> {error, no_table};
        {match, [_, _, T]} -> {ok, T}
    end.

get_re() ->
    case get(cqerl_table_compiled_re) of
        undefined -> compile_re();
        X -> X
    end.

compile_re() ->
    Pattern = "[^']*\\s(FROM|INTO|UPDATE)[\\s\\(]+([\\w]+).*",
    {ok, CRE} = re2:compile(Pattern, [caseless]),
    put(cqerl_table_compiled_re, CRE),
    CRE.
