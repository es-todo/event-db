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
RUN apt install neovim -y
RUN wget https://deb.nodesource.com/setup_23.x
RUN sudo -E bash setup_23.x
RUN sudo apt-get install nodejs -y

COPY flyway.toml /flyway.toml
COPY migrations /migrations

RUN mkdir /app
WORKDIR /app
COPY ./package.json /app
COPY ./package-lock.json /app
RUN npm install
COPY ./tsconfig.json /app
COPY ./src /app
RUN npm run check

COPY docker-entrypoint.sh /docker-entrypoint.sh
ENTRYPOINT ["/docker-entrypoint.sh"]
