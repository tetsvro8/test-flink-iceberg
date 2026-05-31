FROM flink:1.20-scala_2.12

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev \
        openjdk-11-jdk-headless \
        build-essential && \
    JAVA_HOME=/usr/lib/jvm/java-11-openjdk-$(dpkg --print-architecture) \
    pip3 install --no-cache-dir apache-flink==1.20.0 && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
