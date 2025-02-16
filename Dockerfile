FROM ubuntu:24.04
RUN apt update -y
RUN apt upgrade -y
RUN apt install postgresql -y
RUN apt install wget -y
RUN wget https://download.red-gate.com/maven/release/com/redgate/flyway/flyway-commandline/11.3.1/flyway-commandline-11.3.1-linux-x64.tar.gz
RUN tar zxvf flyway-commandline-11.3.1-linux-x64.tar.gz
RUN ln -s /flyway-11.3.1/flyway /usr/bin/flyway
RUN ls -l /usr/bin/flyway
RUN apt install sudo -y
COPY flyway.toml /flyway.toml
COPY migrations /migrations
COPY docker-entrypoint.sh /docker-entrypoint.sh
ENTRYPOINT ["/docker-entrypoint.sh"]
