FROM cassandra:3

RUN apt-get update -y && apt-get install -y wget git build-essential
RUN wget https://packages.erlang-solutions.com/erlang-solutions_1.0_all.deb
RUN dpkg -i erlang-solutions_1.0_all.deb
RUN apt-get update -y && apt-get install -y erlang
RUN wget https://s3.amazonaws.com/rebar3/rebar3 && chmod +x rebar3
RUN mv rebar3 /bin/rebar3

WORKDIR /opt/cqerl
COPY . ./

CMD [ "rebar3" ]
