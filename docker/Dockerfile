FROM ubuntu:18.04

# Update packages and setup timezone
RUN apt-get update && apt-get -y upgrade && \
      apt-get -y install tzdata

ENV TZ=Europe/Zurich
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
      echo $TZ > /etc/timezone
RUN dpkg-reconfigure --frontend=noninteractive tzdata


RUN apt-get -y install file unzip zip xz-utils git \
                         gcc g++ cmake default-jdk \
                         python3

ADD docker/apache-maven-3.6.3-bin.tar.gz /usr/local
RUN ln -s /usr/local/apache-maven-3.6.3/bin/mvn /usr/local/bin
COPY template_cpp /root/template_cpp
COPY template_java /root/template_java
COPY 308542 /root/308542
COPY 308542_java /root/308542_java
ADD barrier.py /root
ADD finishedSignal.py /root

# RUN /root/template_cpp/build.sh && /root/template_cpp/cleanup.sh
# RUN /root/template_java/build.sh && /root/template_java/cleanup.sh
RUN /root/308542_java/build.sh && /root/308542_java/cleanup.sh
