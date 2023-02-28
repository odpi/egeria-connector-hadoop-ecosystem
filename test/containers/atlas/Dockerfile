FROM ubuntu:22.04 as build

# Install additional tools
RUN apt-get update && apt-get install -y git python3 openjdk-8-jdk maven build-essential python-is-python3

# Setup environment for Java/Maven
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV MAVEN_HOME /usr/share/maven

# Add Java and Maven to the path.
ENV PATH /usr/java/bin:/usr/local/apache-maven/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Working directory
WORKDIR /root

# Pull down Atlas and build it into /root/atlas-bin.
RUN git clone https://github.com/apache/atlas.git -b master

RUN echo 'package-lock=false' >> ./atlas/.npmrc

RUN echo 'package-lock.json' >> ./atlas/.gitignore

# Memory requirements
ENV MAVEN_OPTS "-Xms2g -Xmx2g"
# RUN export MAVEN_OPTS="-Xms2g -Xmx2g"

# -- Full build -- could be cut down if needed

# Remove -DskipTests if unit tests are to be included
RUN mvn clean install -Dmaven.wagon.http.ssl.ignore.validity.dates=true -Dmaven.wagon.http.ssl.insecure=true -Drat.skip=true -Dcheckstyle.skip=true -Dfindbugs.skip=true -DskipTests -Pdist,embedded-hbase-solr -f ./atlas/pom.xml
RUN mkdir -p atlas-bin
RUN tar xzf /root/atlas/distro/target/*bin.tar.gz --strip-components 1 -C /root/atlas-bin

# --- RUN time ---

#TODO: Light weight runtime needed - but needs to be JDK as atlas uses jar
FROM docker.io/eclipse-temurin:8-jdk

LABEL org.label-schema.schema-version = "1.0"
LABEL org.label-schema.vendor = "LF AI & Data"
LABEL org.label-schema.name = "apache-atlas"
LABEL org.label-schema.description = "Apache Atlas image to support LF AI & Data Egeria development and demos."
LABEL org.label-schema.url = "https://github.com/planetf1/atlas-docker"
LABEL org.label-schema.vcs-url = "https://planetf1/atlas-docker"
LABEL org.label-schema.docker.cmd = "docker run -d -p 21000:21000 $REPO/atlas"
LABEL org.label-schema.docker.debug = "docker exec -it $CONTAINER /bin/sh"

ENV JAVA_TOOL_OPTIONS="-Xmx2048m" \
    HBASE_CONF_DIR="/opt/apache/atlas/hbase/conf" \
    ATLAS_OPTS="-Dkafka.advertised.hostname=localhost"

RUN apt-get update && apt-get install -y python3 python-is-python3 bash && \
    apt-get -y  dist-upgrade && \
    groupadd -r atlas -g 21000 && \
    useradd --no-log-init -r -g atlas -u 21000 -d /opt/apache/atlas atlas

COPY --from=build --chown=atlas:atlas /root/atlas-bin/ /opt/apache/atlas/

# Must use numeric userid here to meet k8s security checks
USER 21000:21000

WORKDIR /opt/apache/atlas
RUN sed -i "s|^atlas.graph.storage.lock.wait-time=10000|atlas.graph.storage.lock.wait-time=200|g" conf/atlas-application.properties && \
    echo "atlas.notification.relationships.enabled=true" >> conf/atlas-application.properties && \
    echo "atlas.kafka.listeners=PLAINTEXT://:9027" >> conf/atlas-application.properties && \
    echo "atlas.kafka.advertised.listeners=PLAINTEXT://\${sys:kafka.advertised.hostname}:9027" >> conf/atlas-application.properties

EXPOSE 9026 9027 21000

ENV JAVA_HOME="/opt/java/openjdk"
ENTRYPOINT ["/bin/bash", "-c", "/opt/apache/atlas/bin/atlas_start.py; tail -fF /opt/apache/atlas/logs/atlas*.out"]
