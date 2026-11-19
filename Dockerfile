FROM eclipse-temurin:25-jre
RUN groupadd --system submission && useradd --system --gid submission --home-dir /app submission
WORKDIR /app
COPY --chown=submission:submission build/libs/submission-service.jar /app/submission-service.jar
USER submission
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "/app/submission-service.jar"]
