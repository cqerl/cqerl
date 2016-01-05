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

PLT_FILE = cqerl.plt
OTP_APPS = kernel stdlib ssl compiler erts crypto public_key hipe inets asn1 mnesia runtime_tools syntax_tools

$(PLT_FILE):
	dialyzer --build_plt --apps $(OTP_APPS) --output_plt $(PLT_FILE) deps/*/ebin

dialyzer: recompile $(PLT_FILE)
	dialyzer --fullpath --plt $(PLT_FILE) ./ebin
