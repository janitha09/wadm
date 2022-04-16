# See here for image contents: https://github.com/microsoft/vscode-dev-containers/tree/v0.194.3/containers/ubuntu/.devcontainer/base.Dockerfile

# [Choice] Ubuntu version: hirsute, bionic, focal
ARG VARIANT="hirsute"
FROM mcr.microsoft.com/vscode/devcontainers/base:0-${VARIANT}

# [Optional] Uncomment this section to install additional OS packages.
RUN sudo wget https://packages.erlang-solutions.com/erlang-solutions_2.0_all.deb -O /tmp/erlang-solutions_2.0_all.deb \
    && sudo dpkg -i /tmp/erlang-solutions_2.0_all.deb \
    && apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install build-essential esl-erlang elixir
    # && curl -L https://github.com/nats-io/nats-server/releases/download/v2.0.0/nats-server-v2.0.0-linux-amd64.zip -o nats-server.zip \
    # && unzip nats-server.zip -d nats-server \
    # && sudo cp nats-server/nats-server-v2.0.0-linux-amd64/nats-server /usr/bin