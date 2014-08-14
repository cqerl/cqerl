.PHONY: all test clean
REBAR=./rebar

all: deps compile

deps:
	${REBAR} get-deps

compile:
	${REBAR} compile

recompile:
	${REBAR} skip_deps=true compile

test:
	${REBAR} skip_deps=true ct

clean:
	${REBAR} clean
