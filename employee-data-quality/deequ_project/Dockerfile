FROM openjdk:11-jdk-slim

# Install necessary tools
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    bash \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install Spark
ENV SPARK_VERSION=3.3.4
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Install SBT
RUN curl -L -o sbt.deb https://repo.scala-sbt.org/scalasbt/debian/sbt-1.8.2.deb \
    && dpkg -i sbt.deb \
    && rm sbt.deb

# Set environment variables
ENV PATH=${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}
ENV PYSPARK_PYTHON=python3

# Copy project files
COPY . .

# Download Deequ jar
RUN mkdir -p /app/lib && \
    wget https://repo1.maven.org/maven2/com/amazon/deequ/deequ/2.0.1-spark-3.2/deequ-2.0.1-spark-3.2.jar -O /app/lib/deequ.jar

# Make scripts executable
RUN chmod +x *.sh

# Create output directory
RUN mkdir -p /app/output

CMD ["/app/run_deequ_validation.sh"]