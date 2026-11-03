FROM debian:12 as build

RUN apt update
RUN apt install -y openssl git make clang build-essential libssl-dev

WORKDIR /opt
COPY src ./src
COPY Makefile .
COPY proxy.conf .
RUN make

FROM debian:12
RUN apt update 
RUN apt install -y openssl
COPY --from=build /opt/valkey-proxy /usr/local/bin/valkey-proxy
CMD [ "valkey-proxy" ]
