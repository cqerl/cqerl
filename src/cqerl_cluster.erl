-module (cqerl_cluster).

-export([
    init/1,
    terminate/2,
    code_change/3,

    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-export([
	start_link/0,
	get_any_from_cluster/1,
	get_any/0,
    add_clients_to_cluster/2,
    add_clients_to_cluster/3
]).

-define (PRIMARY_CLUSTER, '$primary_cluster').

-record(cluster_table, {
          key :: cqerl_hash:key(),
          client_key :: cqerl_hash:key()
         }).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

add_clients_to_cluster(Key, ClientKeys) ->
    gen_server:cast(?MODULE, {add_to_cluster, Key, ClientKeys}).

add_clients_to_cluster(Key, ClientKeys, Opts0) ->
	add_clients_to_cluster(Key, lists:map(fun
		({Inet, Opts}) when is_list(Opts) ->
			{Inet, Opts ++ Opts0};
		(Inet) ->
			{Inet, Opts0}
	end, ClientKeys)).

get_any_from_cluster(Key) ->
	case ets:lookup(cqerl_clusters, Key) of
		[] -> {error, cluster_not_configured};
		Nodes ->
			Table = lists:nth(random:uniform(length(Nodes)), Nodes),
			cqerl_hash:get_client(Table#cluster_table.client_key)
	end.

get_any() ->
	get_any_from_cluster(?PRIMARY_CLUSTER).

init(_) ->
    ets:new(cqerl_clusters, [named_table, {read_concurrency, true}, protected, 
                             {keypos, #cluster_table.key}, bag]),
    {ok, undefined, 0}.

handle_cast({add_to_cluster, ClusterKey, ClientKeys}, _State) ->
	Tables = ets:lookup(cqerl_clusters, ClusterKey),
    AlreadyStarted = sets:from_list(lists:map(fun
    	(#cluster_table{client_key=ClientKey}) -> ClientKey
    end, Tables)),
    NewClients = sets:subtract(sets:from_list(ClientKeys), AlreadyStarted),
    lists:map(fun
        (Key) -> 
        	case cqerl_hash:get_client(Key) of
        		{ok, _} ->
        			ets:insert(cqerl_clusters, #cluster_table{key=ClusterKey, client_key=Key});
        		{error, Reason} ->
        			io:format(standard_error, "Error while starting client ~p for cluster ~p:~n~p", [Key, ClusterKey, Reason])
        	end
    end, sets:to_list(NewClients)),
    {noreply, undefined}.

handle_info(timeout, _State) ->
	case application:get_env(cqerl, cassandra_clusters, undefined) of
    	undefined ->
    		case application:get_env(cqerl, cassandra_nodes, undefined) of
    			undefined -> ok;
    			ClientKeys when is_list(ClientKeys) ->
    				handle_cast({add_to_cluster, ?PRIMARY_CLUSTER, ClientKeys}, undefined)
    		end;

    	Clusters when is_map(Clusters) ->
    		maps:map(fun
    			({ClusterKey, {ClientKeys, Opts0}}) when is_list(ClientKeys) ->
    				handle_cast({add_to_cluster, ClusterKey, lists:map(fun
						({Inet, Opts}) when is_list(Opts) ->
							{Inet, Opts ++ Opts0};
						(Inet) ->
							{Inet, Opts0}
					end, ClientKeys)}, undefined);

				({ClusterKey, ClientKeys}) when is_list(ClientKeys) ->
    				handle_cast({add_to_cluster, ClusterKey, ClientKeys}, undefined)
    		end, Clusters)
    end,
    {noreply, undefined};

handle_info(_Msg, _State) -> {noreply, undefined}.

handle_call(_Msg, _From, _State) -> {reply, {error, unexpected_message}, undefined}.

code_change(_OldVsn, _State, _Extra) -> {ok, undefined}.

terminate(_Reason, _State) ->
	ok.
