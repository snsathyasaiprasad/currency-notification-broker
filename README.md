Real-Time Currency Exchange & Notification Broker

Complete Build Guide — From Blank Debian Box to Kubernetes + Jenkins CI/CD

This guide assumes zero prior setup. Every command is meant to be copy-pasted into a terminal on Debian. Where something needs explaining, it's explained before the command, not after.


0. Architecture (what you're actually building)

Two independent Spring Boot microservices that never call each other directly — they only talk through Kafka (the "broker" in your project name):

 [Frankfurter API]                                   [User via REST/WebSocket]
        │  (fetch rates every N seconds)                      ▲
        ▼                                                     │
 ┌─────────────────┐      Kafka topic          ┌───────────────────────┐
 │ currency-service │ ───"currency-rates"────▶ │ notification-service   │
 │ (Spring Boot)    │      (JSON messages)     │ (Spring Boot)          │
 │ - REST API       │                          │ - Kafka consumer       │
 │ - Postgres       │                          │ - compares rate vs     │
 │   (rate history) │                          │   user thresholds      │
 └─────────────────┘                           │ - Postgres (thresholds)│
                                               │ - pushes alerts over   │
                                               │   WebSocket + logs     │
                                               └───────────────────────┘


currency-service: polls a free exchange-rate API on a schedule, saves each rate to Postgres, and publishes every update onto a Kafka topic.
notification-service: listens to that Kafka topic, checks each rate against thresholds users have registered via REST, and pushes an alert (WebSocket message, logged here — you can wire in email/SMS later).
Kafka + Zookeeper: the message broker connecting them.
Postgres: one instance, two schemas/databases (one per service — microservices should never share a database).


You will run all of this locally with Docker Compose first (fast feedback loop), then package it for Kubernetes, then automate the whole build/deploy with Jenkins.


1. Install everything on Debian

Open a terminal. Run these in order.

1.1 Update the system

bashsudo apt update && sudo apt upgrade -y
sudo apt install -y curl wget git unzip build-essential apt-transport-https ca-certificates gnupg software-properties-common

1.2 Java 17 (Spring Boot 3.x needs Java 17+)

bashsudo apt install -y openjdk-17-jdk
java -version   # should print "17.x.x"

1.3 Maven (builds the Spring Boot projects)

bashsudo apt install -y maven
mvn -version

1.4 VS Code

If not already installed:

bashwget -O vscode.deb "https://go.microsoft.com/fwlink/?LinkID=760868"
sudo apt install -y ./vscode.deb

Then open VS Code and install these extensions (Extensions panel, Ctrl+Shift+X): Extension Pack for Java, Spring Boot Extension Pack, Docker, Kubernetes, YAML.

1.5 Git

bashsudo apt install -y git
git config --global user.name "Your Name"
git config --global user.email "you@example.com"

1.6 Docker Engine + Docker Compose

bash# Remove any old versions
sudo apt remove -y docker docker-engine docker.io containerd runc

# Add Docker's official repo
curl -fsSL https://download.docker.com/linux/debian/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list

sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Let your user run docker without sudo
sudo usermod -aG docker $USER
newgrp docker

# Verify
docker --version
docker compose version
docker run hello-world

1.7 kubectl (talks to a Kubernetes cluster)

bashcurl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
kubectl version --client

1.8 minikube (a real local Kubernetes cluster on your machine)

bashcurl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube
minikube start --driver=docker --cpus=4 --memory=6g
kubectl get nodes   # should show one "Ready" node

1.9 GitHub CLI (optional but makes repo creation from terminal easy)

bashcurl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | sudo dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg
sudo chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null
sudo apt update && sudo apt install -y gh
gh auth login   # follow the browser prompt

At this point your machine has: Java, Maven, Git, Docker, kubectl, minikube, GitHub CLI, VS Code. That's the full toolchain.


2. GitHub repository setup

bashmkdir -p ~/projects/currency-notification-broker
cd ~/projects/currency-notification-broker
git init
gh repo create currency-notification-broker --public --source=. --remote=origin

(If you don't want to use gh, just create the repo manually on github.com and run git remote add origin git@github.com:<you>/currency-notification-broker.git instead.)

Create the monorepo layout:

bashmkdir -p currency-service notification-service k8s jenkins

Add a root .gitignore:

bashcat > .gitignore << 'EOF'
target/
*.class
.idea/
*.iml
.vscode/
.env
EOF


3. currency-service (Spring Boot)

3.1 Generate the project

Use Spring Initializr from the command line (no browser needed):

bashcd ~/projects/currency-notification-broker
curl https://start.spring.io/starter.zip \
  -d dependencies=web,data-jpa,postgresql,kafka,actuator,validation,lombok \
  -d type=maven-project \
  -d language=java \
  -d bootVersion=3.3.0 \
  -d baseDir=currency-service \
  -d groupId=com.broker \
  -d artifactId=currency-service \
  -d name=currency-service \
  -d packageName=com.broker.currency \
  -d javaVersion=17 \
  -o currency-service.zip
unzip currency-service.zip -d currency-service
rm currency-service.zip

3.2 Configuration — currency-service/src/main/resources/application.yml

Delete the auto-generated application.properties and create this instead:

yamlserver:
  port: 8081

spring:
  application:
    name: currency-service
  datasource:
    url: jdbc:postgresql://${DB_HOST:localhost}:5432/${DB_NAME:currency_db}
    username: ${DB_USER:currency_user}
    password: ${DB_PASS:currency_pass}
  jpa:
    hibernate:
      ddl-auto: update
    show-sql: false
  kafka:
    bootstrap-servers: ${KAFKA_BROKER:localhost:9092}
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer

currency:
  base: USD
  targets: EUR,GBP,INR,JPY,AUD
  poll-interval-ms: 15000
  topic: currency-rates

management:
  endpoints:
    web:
      exposure:
        include: health,info

3.3 Entity — src/main/java/com/broker/currency/model/ExchangeRate.java

javapackage com.broker.currency.model;

import jakarta.persistence.*;
import lombok.*;
import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(name = "exchange_rates")
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class ExchangeRate {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String baseCurrency;
    private String targetCurrency;
    private BigDecimal rate;
    private Instant fetchedAt;
}

3.4 Repository — src/main/java/com/broker/currency/repository/ExchangeRateRepository.java

javapackage com.broker.currency.repository;

import com.broker.currency.model.ExchangeRate;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface ExchangeRateRepository extends JpaRepository<ExchangeRate, Long> {
    List<ExchangeRate> findTop20ByBaseCurrencyAndTargetCurrencyOrderByFetchedAtDesc(
        String base, String target);
}

3.5 DTO for the Frankfurter API response — src/main/java/com/broker/currency/client/FrankfurterResponse.java

javapackage com.broker.currency.client;

import java.util.Map;

public record FrankfurterResponse(String base, String date, Map<String, Double> rates) {}

3.6 Kafka message DTO — src/main/java/com/broker/currency/dto/RateEvent.java

javapackage com.broker.currency.dto;

import java.math.BigDecimal;
import java.time.Instant;

public record RateEvent(String base, String target, BigDecimal rate, Instant timestamp) {}

3.7 The scheduled fetch + publish service — src/main/java/com/broker/currency/service/RateFetchService.java

javapackage com.broker.currency.service;

import com.broker.currency.client.FrankfurterResponse;
import com.broker.currency.dto.RateEvent;
import com.broker.currency.model.ExchangeRate;
import com.broker.currency.repository.ExchangeRateRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class RateFetchService {

    private final ExchangeRateRepository repository;
    private final KafkaTemplate<String, RateEvent> kafkaTemplate;
    private final RestClient restClient = RestClient.create("https://api.frankfurter.dev");

    @Value("${currency.base}")
    private String base;

    @Value("${currency.targets}")
    private String targetsCsv;

    @Value("${currency.topic}")
    private String topic;

    @Scheduled(fixedDelayString = "${currency.poll-interval-ms}")
    public void fetchAndPublish() {
        List<String> targets = List.of(targetsCsv.split(","));
        String symbols = String.join(",", targets);

        FrankfurterResponse response = restClient.get()
            .uri("/v1/latest?base={base}&symbols={symbols}", base, symbols)
            .retrieve()
            .body(FrankfurterResponse.class);

        if (response == null || response.rates() == null) {
            log.warn("No rate data returned from provider");
            return;
        }

        response.rates().forEach((target, rateValue) -> {
            BigDecimal rate = BigDecimal.valueOf(rateValue);
            Instant now = Instant.now();

            repository.save(ExchangeRate.builder()
                .baseCurrency(base)
                .targetCurrency(target)
                .rate(rate)
                .fetchedAt(now)
                .build());

            RateEvent event = new RateEvent(base, target, rate, now);
            kafkaTemplate.send(topic, base + "-" + target, event);
            log.info("Published rate {} -> {} = {}", base, target, rate);
        });
    }
}

3.8 REST controller — src/main/java/com/broker/currency/controller/RateController.java

javapackage com.broker.currency.controller;

import com.broker.currency.model.ExchangeRate;
import com.broker.currency.repository.ExchangeRateRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/rates")
@RequiredArgsConstructor
public class RateController {

    private final ExchangeRateRepository repository;

    @GetMapping("/{base}/{target}/history")
    public List<ExchangeRate> history(@PathVariable String base, @PathVariable String target) {
        return repository.findTop20ByBaseCurrencyAndTargetCurrencyOrderByFetchedAtDesc(base, target);
    }
}

3.9 Enable scheduling — edit src/main/java/com/broker/currency/CurrencyServiceApplication.java

javapackage com.broker.currency;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class CurrencyServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(CurrencyServiceApplication.class, args);
    }
}

3.10 Kafka JSON serialization config — add a bean so Kafka trusts your package

src/main/java/com/broker/currency/config/KafkaConfig.java

javapackage com.broker.currency.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}

(This replaces the need for a generic KafkaTemplate<String, RateEvent> bean — Spring Boot will autowire the Object one; Java generics erasure makes this fine at runtime.)


4. notification-service (Spring Boot)

4.1 Generate

bashcd ~/projects/currency-notification-broker
curl https://start.spring.io/starter.zip \
  -d dependencies=web,data-jpa,postgresql,kafka,websocket,actuator,validation,lombok \
  -d type=maven-project \
  -d language=java \
  -d bootVersion=3.3.0 \
  -d baseDir=notification-service \
  -d groupId=com.broker \
  -d artifactId=notification-service \
  -d name=notification-service \
  -d packageName=com.broker.notification \
  -d javaVersion=17 \
  -o notification-service.zip
unzip notification-service.zip -d notification-service
rm notification-service.zip

4.2 application.yml

yamlserver:
  port: 8082

spring:
  application:
    name: notification-service
  datasource:
    url: jdbc:postgresql://${DB_HOST:localhost}:5432/${DB_NAME:notification_db}
    username: ${DB_USER:notification_user}
    password: ${DB_PASS:notification_pass}
  jpa:
    hibernate:
      ddl-auto: update
  kafka:
    bootstrap-servers: ${KAFKA_BROKER:localhost:9092}
    consumer:
      group-id: notification-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "*"

notification:
  topic: currency-rates

management:
  endpoints:
    web:
      exposure:
        include: health,info

4.3 Threshold entity — src/main/java/com/broker/notification/model/Threshold.java

javapackage com.broker.notification.model;

import jakarta.persistence.*;
import lombok.*;
import java.math.BigDecimal;

@Entity
@Table(name = "thresholds")
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class Threshold {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String userId;
    private String baseCurrency;
    private String targetCurrency;
    private BigDecimal thresholdValue;

    @Enumerated(EnumType.STRING)
    private Direction direction; // ABOVE or BELOW

    public enum Direction { ABOVE, BELOW }
}

4.4 Repository — src/main/java/com/broker/notification/repository/ThresholdRepository.java

javapackage com.broker.notification.repository;

import com.broker.notification.model.Threshold;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface ThresholdRepository extends JpaRepository<Threshold, Long> {
    List<Threshold> findByBaseCurrencyAndTargetCurrency(String base, String target);
}

4.5 REST controller to register thresholds — src/main/java/com/broker/notification/controller/ThresholdController.java

javapackage com.broker.notification.controller;

import com.broker.notification.model.Threshold;
import com.broker.notification.repository.ThresholdRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/thresholds")
@RequiredArgsConstructor
public class ThresholdController {

    private final ThresholdRepository repository;

    @PostMapping
    public Threshold create(@RequestBody Threshold threshold) {
        return repository.save(threshold);
    }

    @GetMapping
    public Iterable<Threshold> all() {
        return repository.findAll();
    }
}

4.6 WebSocket config (so alerts push live to a browser) — src/main/java/com/broker/notification/config/WebSocketConfig.java

javapackage com.broker.notification.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        registry.enableSimpleBroker("/topic");
        registry.setApplicationDestinationPrefixes("/app");
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/ws").setAllowedOriginPatterns("*").withSockJS();
    }
}

4.7 The Kafka listener that evaluates thresholds and pushes alerts

src/main/java/com/broker/notification/service/RateEventListener.java

javapackage com.broker.notification.service;

import com.broker.notification.model.Threshold;
import com.broker.notification.repository.ThresholdRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class RateEventListener {

    private final ThresholdRepository thresholdRepository;
    private final SimpMessagingTemplate messagingTemplate;

    @KafkaListener(topics = "${notification.topic}", groupId = "notification-group")
    public void onRateEvent(Map<String, Object> event) {
        String base = (String) event.get("base");
        String target = (String) event.get("target");
        BigDecimal rate = new BigDecimal(event.get("rate").toString());

        for (Threshold t : thresholdRepository.findByBaseCurrencyAndTargetCurrency(base, target)) {
            boolean triggered =
                (t.getDirection() == Threshold.Direction.ABOVE && rate.compareTo(t.getThresholdValue()) > 0) ||
                (t.getDirection() == Threshold.Direction.BELOW && rate.compareTo(t.getThresholdValue()) < 0);

            if (triggered) {
                String message = String.format(
                    "ALERT for %s: %s/%s is now %s (threshold %s %s)",
                    t.getUserId(), base, target, rate, t.getDirection(), t.getThresholdValue());
                log.info(message);
                messagingTemplate.convertAndSend("/topic/alerts/" + t.getUserId(), message);
            }
        }
    }
}

That's the full application logic. Both services build independently with Maven.


5. Run everything locally with Docker Compose

5.1 Dockerfile for currency-service — currency-service/Dockerfile

dockerfile# Stage 1: build
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline
COPY src ./src
RUN mvn clean package -DskipTests

# Stage 2: run
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]

Copy the same file to notification-service/Dockerfile, just change EXPOSE 8081 to EXPOSE 8082.

5.2 Root docker-compose.yml (place at repo root)

yamlversion: "3.8"

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.6.0
    depends_on: [zookeeper]
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_MULTIPLE_DATABASES: currency_db,notification_db
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    ports:
      - "5432:5432"
    volumes:
      - ./scripts/init-multi-db.sh:/docker-entrypoint-initdb.d/init-multi-db.sh

  currency-service:
    build: ./currency-service
    depends_on: [kafka, postgres]
    ports:
      - "8081:8081"
    environment:
      DB_HOST: postgres
      DB_NAME: currency_db
      DB_USER: postgres
      DB_PASS: postgres
      KAFKA_BROKER: kafka:9092

  notification-service:
    build: ./notification-service
    depends_on: [kafka, postgres]
    ports:
      - "8082:8082"
    environment:
      DB_HOST: postgres
      DB_NAME: notification_db
      DB_USER: postgres
      DB_PASS: postgres
      KAFKA_BROKER: kafka:9092

5.3 Script to create two databases in one Postgres container

scripts/init-multi-db.sh

bash#!/bin/bash
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE currency_db;
    CREATE DATABASE notification_db;
EOSQL

Make it executable: chmod +x scripts/init-multi-db.sh

5.4 Bring it all up

bashcd ~/projects/currency-notification-broker
docker compose up --build

Wait ~30–60 seconds for Kafka to be ready. Then test:

bashcurl http://localhost:8081/api/rates/USD/EUR/history
curl -X POST http://localhost:8082/api/thresholds \
  -H "Content-Type: application/json" \
  -d '{"userId":"sathya","baseCurrency":"USD","targetCurrency":"EUR","thresholdValue":0.80,"direction":"ABOVE"}'

Watch the notification-service logs — within one poll cycle (15s) you'll see ALERT for sathya: ... if the live rate crosses your threshold.

Push this working state to GitHub now:

bashgit add .
git commit -m "Working currency-service and notification-service with docker-compose"
git push -u origin main


6. Push images to Docker Hub

Kubernetes needs to pull images from somewhere — Docker Hub is the easiest for a student project.

bash# create a free account at hub.docker.com first, then:
docker login

docker build -t <your-dockerhub-username>/currency-service:1.0 ./currency-service
docker build -t <your-dockerhub-username>/notification-service:1.0 ./notification-service

docker push <your-dockerhub-username>/currency-service:1.0
docker push <your-dockerhub-username>/notification-service:1.0


7. Kubernetes manifests

Everything below goes in the k8s/ folder. minikube is already running from step 1.8.

7.1 Namespace — k8s/00-namespace.yaml

yamlapiVersion: v1
kind: Namespace
metadata:
  name: currency-broker

7.2 Postgres — k8s/01-postgres.yaml

yamlapiVersion: v1
kind: ConfigMap
metadata:
  name: postgres-init
  namespace: currency-broker
data:
  init-multi-db.sh: |
    #!/bin/bash
    set -e
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        CREATE DATABASE currency_db;
        CREATE DATABASE notification_db;
    EOSQL
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
  namespace: currency-broker
spec:
  replicas: 1
  selector:
    matchLabels: { app: postgres }
  template:
    metadata:
      labels: { app: postgres }
    spec:
      containers:
        - name: postgres
          image: postgres:16-alpine
          env:
            - name: POSTGRES_USER
              value: postgres
            - name: POSTGRES_PASSWORD
              value: postgres
          ports:
            - containerPort: 5432
          volumeMounts:
            - name: init-script
              mountPath: /docker-entrypoint-initdb.d
      volumes:
        - name: init-script
          configMap:
            name: postgres-init
            defaultMode: 0755
---
apiVersion: v1
kind: Service
metadata:
  name: postgres
  namespace: currency-broker
spec:
  selector: { app: postgres }
  ports:
    - port: 5432

7.3 Kafka + Zookeeper — k8s/02-kafka.yaml

yamlapiVersion: apps/v1
kind: Deployment
metadata:
  name: zookeeper
  namespace: currency-broker
spec:
  replicas: 1
  selector:
    matchLabels: { app: zookeeper }
  template:
    metadata:
      labels: { app: zookeeper }
    spec:
      containers:
        - name: zookeeper
          image: confluentinc/cp-zookeeper:7.6.0
          env:
            - name: ZOOKEEPER_CLIENT_PORT
              value: "2181"
          ports:
            - containerPort: 2181
---
apiVersion: v1
kind: Service
metadata:
  name: zookeeper
  namespace: currency-broker
spec:
  selector: { app: zookeeper }
  ports:
    - port: 2181
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka
  namespace: currency-broker
spec:
  replicas: 1
  selector:
    matchLabels: { app: kafka }
  template:
    metadata:
      labels: { app: kafka }
    spec:
      containers:
        - name: kafka
          image: confluentinc/cp-kafka:7.6.0
          env:
            - name: KAFKA_BROKER_ID
              value: "1"
            - name: KAFKA_ZOOKEEPER_CONNECT
              value: "zookeeper:2181"
            - name: KAFKA_ADVERTISED_LISTENERS
              value: "PLAINTEXT://kafka:9092"
            - name: KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR
              value: "1"
          ports:
            - containerPort: 9092
---
apiVersion: v1
kind: Service
metadata:
  name: kafka
  namespace: currency-broker
spec:
  selector: { app: kafka }
  ports:
    - port: 9092

7.4 currency-service — k8s/03-currency-service.yaml

yamlapiVersion: apps/v1
kind: Deployment
metadata:
  name: currency-service
  namespace: currency-broker
spec:
  replicas: 2
  selector:
    matchLabels: { app: currency-service }
  template:
    metadata:
      labels: { app: currency-service }
    spec:
      containers:
        - name: currency-service
          image: <your-dockerhub-username>/currency-service:1.0
          ports:
            - containerPort: 8081
          env:
            - name: DB_HOST
              value: postgres
            - name: DB_NAME
              value: currency_db
            - name: DB_USER
              value: postgres
            - name: DB_PASS
              value: postgres
            - name: KAFKA_BROKER
              value: kafka:9092
---
apiVersion: v1
kind: Service
metadata:
  name: currency-service
  namespace: currency-broker
spec:
  selector: { app: currency-service }
  ports:
    - port: 8081
  type: NodePort

7.5 notification-service — k8s/04-notification-service.yaml

yamlapiVersion: apps/v1
kind: Deployment
metadata:
  name: notification-service
  namespace: currency-broker
spec:
  replicas: 2
  selector:
    matchLabels: { app: notification-service }
  template:
    metadata:
      labels: { app: notification-service }
    spec:
      containers:
        - name: notification-service
          image: <your-dockerhub-username>/notification-service:1.0
          ports:
            - containerPort: 8082
          env:
            - name: DB_HOST
              value: postgres
            - name: DB_NAME
              value: notification_db
            - name: DB_USER
              value: postgres
            - name: DB_PASS
              value: postgres
            - name: KAFKA_BROKER
              value: kafka:9092
---
apiVersion: v1
kind: Service
metadata:
  name: notification-service
  namespace: currency-broker
spec:
  selector: { app: notification-service }
  ports:
    - port: 8082
  type: NodePort

7.6 Apply everything

bashkubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/01-postgres.yaml
kubectl apply -f k8s/02-kafka.yaml
kubectl apply -f k8s/03-currency-service.yaml
kubectl apply -f k8s/04-notification-service.yaml

kubectl get pods -n currency-broker -w   # watch until all say "Running"

7.7 Access the services

bashminikube service currency-service -n currency-broker --url
minikube service notification-service -n currency-broker --url

Use the printed URLs the same way you used localhost:8081 / localhost:8082 earlier.

Commit the k8s/ folder to GitHub too.


8. Jenkins — CI/CD pipeline

8.1 Run Jenkins in Docker (simplest install on Debian)

bashdocker volume create jenkins_home

docker run -d --name jenkins \
  -p 8080:8080 -p 50000:50000 \
  -v jenkins_home:/var/jenkins_home \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -u root \
  jenkins/jenkins:lts

Get the initial admin password:

bashdocker exec jenkins cat /var/jenkins_home/secrets/initialAdminPassword

Open http://localhost:8080, paste the password, choose Install suggested plugins, create your admin user.

Jenkins' container needs the docker CLI to build images. Install it inside the container:

bashdocker exec -u root jenkins bash -c "apt-get update && apt-get install -y docker.io"

8.2 Install extra Jenkins plugins

In Jenkins UI: Manage Jenkins → Plugins → Available → install:


Docker Pipeline
Kubernetes CLI
Git


8.3 Add credentials

Manage Jenkins → Credentials → System → Global credentials → Add Credentials


One of kind "Username with password" — ID: dockerhub-creds — your Docker Hub username/password (or access token).
One of kind "Secret file" — ID: kubeconfig — upload your ~/.kube/config file (this lets Jenkins talk to your minikube cluster; in a real company setup this would point to a shared cluster instead).


8.4 Create the pipeline job

New Item → Pipeline → name it currency-broker-pipeline. Under Pipeline, choose "Pipeline script from SCM", SCM = Git, repo URL = your GitHub repo, script path = Jenkinsfile.

8.5 Jenkinsfile (place at repo root)

groovypipeline {
    agent any

    environment {
        DOCKERHUB_USER = "your-dockerhub-username"
        IMAGE_TAG = "${env.BUILD_NUMBER}"
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Build & Test - currency-service') {
            steps {
                dir('currency-service') {
                    sh 'mvn clean package -DskipTests'
                }
            }
        }

        stage('Build & Test - notification-service') {
            steps {
                dir('notification-service') {
                    sh 'mvn clean package -DskipTests'
                }
            }
        }

        stage('Docker Build & Push') {
            steps {
                withCredentials([usernamePassword(credentialsId: 'dockerhub-creds', usernameVariable: 'DUSER', passwordVariable: 'DPASS')]) {
                    sh '''
                        echo "$DPASS" | docker login -u "$DUSER" --password-stdin
                        docker build -t $DOCKERHUB_USER/currency-service:$IMAGE_TAG ./currency-service
                        docker build -t $DOCKERHUB_USER/notification-service:$IMAGE_TAG ./notification-service
                        docker push $DOCKERHUB_USER/currency-service:$IMAGE_TAG
                        docker push $DOCKERHUB_USER/notification-service:$IMAGE_TAG
                    '''
                }
            }
        }

        stage('Deploy to Kubernetes') {
            steps {
                withCredentials([file(credentialsId: 'kubeconfig', variable: 'KUBECONFIG')]) {
                    sh '''
                        sed -i "s|currency-service:.*|currency-service:$IMAGE_TAG|" k8s/03-currency-service.yaml
                        sed -i "s|notification-service:.*|notification-service:$IMAGE_TAG|" k8s/04-notification-service.yaml
                        kubectl apply -f k8s/00-namespace.yaml
                        kubectl apply -f k8s/01-postgres.yaml
                        kubectl apply -f k8s/02-kafka.yaml
                        kubectl apply -f k8s/03-currency-service.yaml
                        kubectl apply -f k8s/04-notification-service.yaml
                    '''
                }
            }
        }
    }

    post {
        success { echo 'Pipeline completed successfully.' }
        failure { echo 'Pipeline failed — check stage logs above.' }
    }
}

Commit and push this file, then click Build Now on the Jenkins job. Every push to main (if you also add a GitHub webhook under Manage Jenkins → System → GitHub pointing at http://<your-ip>:8080/github-webhook/) will now automatically rebuild, re-push images, and redeploy to Kubernetes.


9. End-to-end test checklist


docker compose up locally → confirm rates flow and alerts fire (Section 5.4).
kubectl get pods -n currency-broker → all pods Running.
Hit currency-service's /api/rates/USD/EUR/history through the minikube URL.
POST a threshold to notification-service, watch its pod logs (kubectl logs -f deploy/notification-service -n currency-broker) for the ALERT line.
Push a trivial code change → Jenkins job auto-builds → new pods roll out (kubectl rollout status deploy/currency-service -n currency-broker).



10. Sensible next steps (once the base system works)


Replace the WebSocket-only alert with real email (Spring Mail + Gmail app password) or SMS (Twilio).
Add a lightweight React or Thymeleaf frontend subscribing to /topic/alerts/{userId} for a live dashboard.
Add Ingress + a domain instead of NodePort URLs.
Add Prometheus + Grafana for metrics (Spring Boot Actuator already exposes /actuator/prometheus if you add the micrometer-registry-prometheus dependency).
Replace hardcoded DB passwords with Kubernetes Secrets instead of plain env values in the YAML.
Add unit tests (JUnit + Mockito) and un-skip them in the Jenkinsfile (mvn clean package without -DskipTests).
