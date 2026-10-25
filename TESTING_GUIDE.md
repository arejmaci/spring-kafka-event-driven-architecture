# 🧪 Guía Paso a Paso para la Prueba de Integración End-to-End

Esta guía detalla el procedimiento completo para levantar la infraestructura, ejecutar los microservicios, monitorear los tópicos de Kafka y validar la persistencia en base de datos.

---

## 🛠️ Prerrequisitos

* **Java 17 / 21** instalado.
* **Maven 3.8+** instalado.
* **Docker Desktop** iniciado y funcionando.
* **Postman** (opcional, para enviar peticiones HTTP).

---

## 🚀 Paso 1: Levantar la Infraestructura (Kafka + UI + MySQL)

Desde la raíz del proyecto, ejecuta el siguiente comando para iniciar el clúster de Kafka y Kafka UI en segundo plano:

```bash
docker-compose up -d
```

> **Verificación:** Puedes acceder a Kafka UI ingresando a `http://localhost:8080` en tu navegador.

---

## 📦 Paso 2: Compilar el Proyecto

Compila y empaqueta ambos microservicios desde la raíz del repositorio:

```bash
mvn clean package -DskipTests
```

---

## 🖥️ Paso 3: Abrir Monitores de Consola para Kafka (Terminales 1 y 2)

Abre dos terminales de línea de comandos para inspeccionar el flujo de eventos en tiempo real.

### Terminal 1: Escuchar Tópico de Comandos Entrada (`product-commands`)
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic product-commands --from-beginning
```

### Terminal 2: Escuchar Tópico de Respuestas/Eventos (`product-replies`)
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic product-replies --from-beginning
```

---

## ⚙️ Paso 4: Iniciar Microservicios (Terminales 3 y 4)

### Terminal 3: Iniciar Servicio de Dominio/Consumidor (`product-command`)
```bash
cd product-command
mvn spring-boot:run
```
*(Se ejecutará en el puerto `8081`)*

### Terminal 4: Iniciar API Gateway/Productor (`products-api`)
```bash
cd products-api
mvn spring-boot:run
```
*(Se ejecutará en el puerto `8080`)*

---

## 🧪 Paso 5: Ejecutar Prueba de Integración (Postman / cURL)

Envía una solicitud `POST` para crear un nuevo producto a través de `products-api`:

### Endpoint:
* **Método:** `POST`
* **URL:** `http://localhost:8080/products`
* **Headers:** `Content-Type: application/json`

### Body (JSON):
```json
{
  "name": "Teclado inalambrico",
  "price": 2530.0
}
```

### Respuesta HTTP Esperada (`200 OK`):
```json
{
  "message": "Product creation command sent successfully"
}
```

---

## 🔍 Paso 6: Verificación de Resultados

### 1. Salida en Terminal 1 (`product-commands`):
Deberás observar el comando emitido por `products-api`:
```json
{"type":"CREATE","id":null,"body":{"name":"Teclado inalambrico","price":2530.0}}
```

### 2. Salida en Terminal 2 (`product-replies`):
Deberás observar la confirmación procesada por `product-command`:
```json
{"status":"SUCCESS","message":"Created Product","body":{"id":4,"name":"Teclado inalambrico","price":2530.0}}
```

### 3. Validación en Base de Datos (MySQL):
Conéctate a MySQL (o mediante DBeaver) y consulta la tabla de productos:

```sql
SELECT * FROM db_spring_kafka.product;
```

**Resultado esperado:**
| id | created_at | name | price | updated_at |
| :--- | :--- | :--- | :--- | :--- |
| `4` | `TIMESTAMP` | `Teclado inalambrico` | `2530` | `TIMESTAMP` |

---

## 🛑 Paso 7: Limpieza del Entorno

Para detener la infraestructura cuando termines las pruebas, ejecuta:

```bash
docker-compose down -v
```