## Configuring cassandra and CQErl to use SSL

There are different ways to configure cassandra to accept client connections
exclusively using SSL/TLS. If you use untrusted networks between cassandra and
its clients, it is highly recommended you do this configuration, as a non-SSL plain
username/password credential exchange is not going to make your setup secure anyhow.

#### Generating a keypair

First you need to give Cassandra a SSL key pair to use when communicating with clients
and with other nodes (node-to-node encryption is another topic though, but details can be
found [here][1]). The present directory includes an example of such keypair with a password of
"aaaaaa". In production though, you should however generate your own, with a much stronger 
password. To do so, go to Cassandra's working directory and do the following (assuming a
UNIX host):

```bash
# Generate the keystore
keytool \
    -genkeypair 
    -alias cassandra \
    -keyalg RSA \
    -keysize 1024 \
    -keystore conf/keystore.jks \
    -storepass MyPassword \
    -keypass MyPassword

# Now generate the cert from that keystore
keytool \
    -exportcert \
    -rfc
    -alias cassandra \
    -file cassandra.pem \
    -keystore conf/keystore.jks \
    -storepass MyPassword
```

The first command will ask you for personal details to tie to the certificate. **ENTER THEM ALL**.
As we've had problem having Erlang clients recognize certificates somehow when some details were omitted.

Then, in your `cassandra.yaml` configuration file, you would look for those lines, near the end, and change it
like so:

```yaml
client_encryption_options:
    enabled: true
    keystore: conf/keystore.jks
    keystore_password: MyPassword
```

You should also change the permissions over the `keystore.jks` file so it can only be read by the user running
the cassandra daemon. Now restart Cassandra and let the magic begin.

#### Making CQErl trust Cassandra

The `cassandra.pem` certificate file you made earlier need to be moved where it can be found by cqerl. 
Then, you can configure CQErl to use SSL in different ways.

1. When starting a new client:

      ```erlang
      cqerl:new_client({"127.0.0.1", 9042}, [ %% Configuration for this particular connection :
                                              {auth, {cqerl_auth_plain_handler, [ {"test", "aaa"} ]}}, 
                                              {ssl, [ {cacertfile, "cassandra.pem"} ] } 
                                             ]
                        ).
      ```
    
2. In environment variables, in your `app.config` or `sys.config` file:

      ```erlang
      [
        {cqerl, [
                  {cassandra_nodes, [ { "127.0.0.1", 9042 } ]},
                  {ssl, [ {cacertfile, "cassandra.pem"} ]},
                  {auth, {cqerl_auth_plain_handler, [ {"test", "aaa"} ]}}
                ]},
        ...
      ]
      ```
      
If cqerl can connect to different hosts with different keys, you can combine all of their certificates
and have that combined file used by cqerl:

```bash
cat cert1.pem cert2.pem cert3.pem > trustedcerts.pem
```

[1]: http://www.datastax.com/documentation/cassandra/2.0/webhelp/index.html#cassandra/security/secureSSLNodeToNode_t.html