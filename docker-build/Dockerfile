FROM apache/spark:3.5.1-scala2.12-java11-python3-ubuntu

USER root
RUN useradd -m -u 1000 dbt
WORKDIR /home/dbt

RUN apt update && apt -y install wget python3 python3-pip python3-venv git \
    chromium \
    fonts-noto-cjk \
    fonts-noto-color-emoji \
    xfonts-terminus \
    fonts-dejavu \
    fonts-freefont-ttf \
    fonts-font-awesome \
    fonts-inconsolata \
    fonts-linuxlibertine

# install Node.js v20
# USER dbt
ENV NVM_DIR=/usr/local/nvm
ENV NODE_VERSION=20.16.0

RUN mkdir $NVM_DIR
RUN wget -qO- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.0/install.sh | bash
RUN echo "source $NVM_DIR/nvm.sh \
    && nvm install $NODE_VERSION \
    && nvm alias default $NODE_VERSION \
    && nvm use default" | bash

ENV NODE_PATH=$NVM_DIR/v$NODE_VERSION/lib/node_modules
ENV PATH=$NVM_DIR/versions/node/v$NODE_VERSION/bin:$PATH

# install python packages
RUN python3 -m pip install --upgrade pip
RUN pip3 install --upgrade --no-cache-dir setuptools wheel

COPY ./requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# others
RUN apt update && apt -y install s3fs inotify-tools lsof

COPY ./jars/*.jar $SPARK_HOME/jars/
COPY ./entrypoint.sh .

ENV SPARK_CONF_DIR=$SPARK_HOME/conf
RUN mkdir -p $SPARK_CONF_DIR
RUN chown -R 1000 $SPARK_HOME

# USER 1000
