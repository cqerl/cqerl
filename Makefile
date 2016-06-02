.PHONY: all test clean
REBAR=./rebar3

all: compile

compile:
	${REBAR} compile

recompile:
	${REBAR} compile

test:
	${REBAR} ct

clean:
	${REBAR} clean

dialyzer:
	${REBAR} dialyzer
