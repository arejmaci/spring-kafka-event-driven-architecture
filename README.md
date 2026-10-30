# Spring Boot & Apache Kafka Event-Driven Architecture

[![Java](https://img.shields.io/badge/Java-21-orange?style=flat-square&logo=openjdk)](https://openjdk.org/projects/jdk/21/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.x-brightgreen?style=flat-square&logo=springboot)](https://spring.io/projects/spring-boot)
[![Spring Cloud Stream](https://img.shields.io/badge/Spring%20Cloud%20Stream-Binder-blue?style=flat-square&logo=spring)](https://spring.io/projects/spring-cloud-stream)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-black?style=flat-square&logo=apachekafka)](https://kafka.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker)](https://www.docker.com/)

Arquitectura de referencia orientada a eventos (*Event-Driven Architecture*) lista para producción, construida con **Java 21**, **Spring Boot 3.x**, **Spring Cloud Stream** y **Apache Kafka (en modo KRaft)**. Este proyecto demuestra cómo diseñar sistemas distribuidos desacoplados, tolerantes a fallos y de alto rendimiento.

---

## 🏗️ Arquitectura y Patrones de Diseño

* **Orientación a Eventos (EDA)**: Comunicación asíncrona entre microservicios independientes mediante Apache Kafka como *broker* central de mensajes.
* **Abstracción con Spring Cloud Stream**: Uso del modelo de programación funcional (`Function`, `Consumer`, `Supplier`) para desacoplar la lógica de negocio de los detalles de infraestructura de mensajería.
* **Patrón Request-Reply**: Implementación robusta de comunicación síncrona sobre flujos asíncronos utilizando *Correlation IDs* y tópicos de respuesta (documentado en el [RFC-001](./docs/RFC-001-request-reply-pattern.md)).
* **Apache Kafka en Modo KRaft**: Ejecución sin ZooKeeper para simplificar la topología de despliegue, reducir consumo de recursos y mejorar los tiempos de arranque.

---

## 📂 Estructura del Proyecto

```text
spring-kafka-event-driven-architecture/
├── docker-compose.yml              # Orquestación local (Kafka KRaft + Kafka UI)
├── docs/                           # Especificaciones técnicas y RFCs
│   └── RFC-001-request-reply-pattern.md
├── services/                       # Módulos de microservicios
│   ├── producer-service/           # Productor de eventos y origen de peticiones
│   └── consumer-service/           # Procesador de eventos y manejador de dominio
└── pom.xml                         # Configuración raíz multi-módulo Maven
```

---

## 🚀 Guía de Inicio Rápido

### Prerrequisitos
* **Java 21 (JDK 21)**
* **Maven 3.9+**
* **Docker & Docker Compose**

### 1. Clonar el Repositorio
```bash
git clone https://github.com/arejmaci/spring-kafka-event-driven-architecture.git
cd spring-kafka-event-driven-architecture
```

### 2. Levantar la Infraestructura (Kafka & Kafka UI)
Inicia el clúster de Kafka en modo KRaft y la interfaz gráfica para monitoreo:
```bash
docker compose up -d
```
> 📊 **Kafka UI** estará disponible en [http://localhost:8080](http://localhost:8080) para inspeccionar tópicos, particiones y mensajes en tiempo real.

### 3. Compilar y Ejecutar los Servicios
Compila todo el proyecto multi-módulo:
```bash
mvn clean install
```

Ejecuta los microservicios de forma independiente:
* **Producer Service**:
  ```bash
  cd services/producer-service
  mvn spring-boot:run
  ```
* **Consumer Service**:
  ```bash
  cd services/consumer-service
  mvn spring-boot:run
  ```

---

## 📄 Documentación Técnica

* **[RFC-001: Request-Reply Pattern Implementation](./docs/RFC-001-request-reply-pattern.md)**: Especificación detallada y diagramas de secuencia sobre la implementación del patrón de solicitud-respuesta utilizando Spring Cloud Stream y Kafka.

---

## 🤝 Contribuciones

¡Las contribuciones, issues y sugerencias son siempre bienvenidas! Siente la libertad de abrir un *pull request* o reportar cualquier mejora.

---

## 📝 Licencia

Este proyecto está bajo la Licencia MIT. Consulta el archivo `LICENSE` para más detalles.
