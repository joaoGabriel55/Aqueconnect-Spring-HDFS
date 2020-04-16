FROM openjdk:11
VOLUME /tmp
ADD target/aqueconnect-0.0.1-SNAPSHOT.jar aqueconnect-0.0.1-SNAPSHOT.jar
EXPOSE 7000
ENTRYPOINT ["java","-jar","aqueconnect-0.0.1-SNAPSHOT.jar"]