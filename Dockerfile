FROM python:3.10

ENV SDKMAN_DIR /root/.sdkman
ENV JAVA_VERSION 8.0.392-zulu

RUN apt -y update
RUN apt -y install gcc
RUN apt -y install g++

RUN rm /bin/sh && ln -s /bin/bash /bin/sh
RUN apt -y install zip curl
RUN curl -s "https://get.sdkman.io" | bash
RUN chmod a+x "$SDKMAN_DIR/bin/sdkman-init.sh"
RUN set -x \
    && echo "sdkman_auto_answer=true" > $SDKMAN_DIR/etc/config \
    && echo "sdkman_auto_selfupdate=false" >> $SDKMAN_DIR/etc/config \
    && echo "sdkman_insecure_ssl=false" >> $SDKMAN_DIR/etc/config
RUN source "$SDKMAN_DIR/bin/sdkman-init.sh" && sdk install java $JAVA_VERSION

ENV JAVA_HOME=/root/.sdkman/candidates/java/current

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
