%% @doc Handler for SASL-based authentication with Cassandra
%% 
%% Authentication with cassandra can take any form, as long as it follows
%% <a href="http://en.wikipedia.org/wiki/Simple_Authentication_and_Security_Layer">
%% SASL principles</a>. The actual implementation (what each message will contain)
%% depends on the method used. Cassandra can be configured to use a certain class
%% for authentication, and you can configure CQErl to use custom handler modules
%% to match the server implementation.
%%
%% <code>auth_init/3</code> will be called by CQErl upon receiving a
%% request from the server to start authentication. Arguments are:
%% <ul>
%%   <li>List of arguments set in the application environment for this 
%%       authentication handler</li>
%%   <li>The name of the authenticator Java class used by Cassandra (as <code>binary()</code>)</li>
%%   <li>A <code>{Ip, Port}</code> tuple where <code>Ip</code> is a tuple of integers 
%%       of length 4 (v4) or 16 (v6).</li>
%% </ul>
%%
%% Return type can be <code>{reply, Reply :: binary(), State :: any()}</code> or 
%% <code>{close, Reason :: any()}</code>.
%%
%% <code>auth_handle_challenge/2</code> will be called every time a challenge
%% is received from Cassandra. The arguments are the challenge (as <code>binary()</code>)
%% and state. The return type is the same as <code>auth_init</code>.
%%
%% <code>auth_handle_success/2</code> will be called when Cassandra indicates
%% authentication success. The arguments are a final message sent along the sucess
%% response (as <code>binary()</code>) and state. The return type can be <code>ok</code>
%% or <code>{close, Reason}</code>.
%%
%% <code>auth_handle_error/2</code> will be called when Cassandra replies with a
%% authentication error. The arguments are a final message sent along the error
%% response (as <code>binary()</code>) and state. The return value is not used.

-module(cqerl_auth_handler).

-type inet() :: {{byte(), byte(), byte(), byte()}, integer()} | %% IPv4 address
                {{byte(), byte(), byte(), byte(),               %% IPv6 address
                  byte(), byte(), byte(), byte(),
                  byte(), byte(), byte(), byte(),
                  byte(), byte(), byte(), byte()}, integer()}.

-callback auth_init([any()], binary(), inet()) -> 
  {reply, Response :: binary(), State :: any()} | {close, Reason :: any()}.
  
-callback auth_handle_challenge(binary(), State :: any()) -> 
  {reply, Response :: binary(), State :: any()} | {close, Reason :: any()}.
  
-callback auth_handle_success(binary(), State :: any()) -> 
  ok | {close, Reason :: any()}.

-callback auth_handle_error(binary(), State :: any()) -> 
  no_return().
