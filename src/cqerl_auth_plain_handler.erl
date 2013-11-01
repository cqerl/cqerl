-module(cqerl_auth_plain_handler).
-behaviour(cqerl_auth_handler).

-define(SASL_PASSWORD_AUTH, <<"PasswordAuthenticator">>).

-export([auth_init/3, auth_handle_challenge/2, auth_handle_success/2, auth_handle_error/2]).
-export([encode_plain_credentials/1]).

%% @doc Encodes a proplist structure with <code>username</code> and <code>password</code> keys
%% into the binary structure expected for the SASL PLAIN mechanism. Extracted from Cassandra's 
%% <a href="https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/auth/PasswordAuthenticator.java#L305">
%% PasswordAuthenticator class</a>. The values in the proplist can be binary strings or plain strings (lists).

-spec encode_plain_credentials([term()]) -> binary().
encode_plain_credentials(PropList) ->
  {ok, User} = case proplists:get_value(username, PropList) of
    BinaryU when is_binary(BinaryU) -> {ok, BinaryU};
    StringU when is_list(StringU) -> {ok, list_to_binary(StringU)}
  end,
  {ok, Password} = case proplists:get_value(password, PropList) of
    BinaryP when is_binary(BinaryP) -> {ok, BinaryP};
    StringP when is_list(StringP) -> {ok, list_to_binary(StringP)}
  end,
  << 0, User/binary, 0, Password/binary >>.

auth_init([{Username, Password}], AuthClass, Address) ->
  auth_init([{username, Username}, {password, Password}], AuthClass, Address);

auth_init(Credentials, ?SASL_PASSWORD_AUTH, _Address) ->
  CredentialsBin = encode_plain_credentials(Credentials),
  {reply, CredentialsBin, undefined};

auth_init(_Creds, Method, _Address) -> {close, {wrong_method, Method}}.
auth_handle_challenge(Msg, _State) -> {close, {unexpected_message, Msg}}.
auth_handle_success(_Msg, _State) ->  ok.
auth_handle_error(Msg, _State) -> ok.
