# Use Flink as the base image
FROM flink:1.19.0-scala_2.12-java11

# Copy custom libraries
COPY lib /opt/flink/lib

# Set environment variables for Hadoop version and download URL
ENV APACHE_HADOOP_URL=https://archive.apache.org/dist/hadoop/ \
    HADOOP_VERSION=2.8.5 \
    HADOOP_HOME=/opt/hadoop

# Install dependencies (wget, tar, and bash)
RUN apt-get update && apt-get install -y wget tar bash && \
    rm -rf /var/lib/apt/lists/*

# Download and extract Hadoop
RUN wget ${APACHE_HADOOP_URL}/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar xzvf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} $HADOOP_HOME && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

# Set HADOOP_CLASSPATH as an environment variable globally
RUN HADOOP_CLASSPATH=$(/opt/hadoop/bin/hadoop classpath) && \
    echo "export HADOOP_CLASSPATH=$HADOOP_CLASSPATH" >> /etc/profile.d/hadoop.sh && \
    echo "export HADOOP_CLASSPATH=$HADOOP_CLASSPATH" >> /etc/environment

# Add Hadoop bin and sbin to PATH
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Do not change the entrypoint or CMD, let the base image start Flink services

