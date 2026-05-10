# Taller de Pruebas de Integración

**Arquitecturas Reactivas con RabbitMQ y Kafka**

---

## Portada

| Campo | Detalle |
|---|---|
| Materia | (completar) |
| Docente | (completar) |
| Integrantes | (completar) |
| Fecha de entrega | (completar) |
| Repositorio | (completar) |

---

## 1. Introducción

Para este taller construimos dos pruebas de concepto que muestran cómo funcionan las arquitecturas basadas en mensajes y eventos. La idea era simple: tener una API que reciba pedidos y, en lugar de procesarlos directamente, dejarlos en manos de un broker para que otros servicios los consuman cuando puedan.

Hicimos dos versiones del mismo caso de uso, una con cada broker:

- Con **RabbitMQ**, que funciona como una cola de mensajes tradicional. Una API (`orders-api`) recibe el pedido y deposita un mensaje `OrderCreatedMessage` en la cola `pedidos`. Un trabajador (`notification-worker`) lo recoge, lo procesa y confirma que terminó.
- Con **Apache Kafka**, que funciona más como un registro de eventos. La misma API publica un evento `OrderCreated` en el tópico `orders.events`, y **tres consumidores diferentes** (`inventory`, `billing`, `notification`) lo reciben en paralelo, cada uno en su propio grupo. Cada uno reacciona al mismo evento, pero hace cosas distintas.

Ambas pruebas corren en Docker Compose y exponen la misma API en el puerto 3000. Sobre las dos arquitecturas ejecutamos los 12 casos de prueba que pide el taller: 6 funcionales y 6 sobre las propiedades reactivas (responsive, resilient, elastic, message-driven, desacoplamiento temporal y recuperación). Los 12 pasaron, y en CP-10 nos llevamos un hallazgo interesante sobre cómo Kafka maneja el escalamiento que documentamos en las conclusiones.

---

## 2. Montaje del entorno

### 2.1 RabbitMQ POC

La arquitectura quedó con 3 contenedores en `rabbitmq-poc/docker-compose.yml`:

| Servicio | Imagen | Puertos |
|---|---|---|
| `rabbitmq` | `rabbitmq:3.13-management` | 5672, 15672 |
| `orders-api` | Node.js 20 (custom) | 3000 |
| `notification-worker` | Node.js 20 (custom) | — |

Para verificar que todo arrancó correctamente, corrimos `docker compose ps`:

![Stack de RabbitMQ levantado](evidencias/rabbitmq/01-contenedores-corriendo.png)

*Figura 1. Stack de RabbitMQ levantado: los 3 contenedores en estado healthy y con sus puertos expuestos.*

### 2.2 Kafka POC

Acá la cosa creció a 6 contenedores en `kafka-poc/docker-compose.yml`:

| Servicio | Imagen | Puertos |
|---|---|---|
| `kafka` | `bitnamilegacy/kafka:3.7` (KRaft) | 29092 |
| `kafka-ui` | `provectuslabs/kafka-ui:latest` | 8080 |
| `orders-api` | Node.js 20 (custom) | 3000 |
| `inventory-consumer` | Node.js 20 (custom) | — |
| `billing-consumer` | Node.js 20 (custom) | — |
| `notification-consumer` | Node.js 20 (custom) | — |

![Stack de Kafka levantado](evidencias/kafka/12-contenedores-kafka.png)

*Figura 2. Stack de Kafka levantado: el broker, la consola Kafka UI, la API productora y los 3 consumidores independientes.*

> 📝 **Hallazgo durante la instalación:** la imagen `bitnami/kafka:3.7` que aparece en muchos tutoriales y en la guía base ya no existe en Docker Hub — Bitnami la sacó del repositorio oficial. La reemplazamos por `bitnamilegacy/kafka:3.7`, que es exactamente la misma imagen pero en el namespace nuevo. No hubo que cambiar nada de configuración.

---

## 3. Casos de prueba

### Resumen de resultados

Antes de entrar en el detalle, este es el resumen de los 12 casos:

| ID | Caso | Arquitectura | Estado |
|---|---|---|---|
| CP-01 | Creación de pedido vía API | RabbitMQ | ✅ Cumple |
| CP-02 | Envío de mensaje a la cola | RabbitMQ | ✅ Cumple |
| CP-03 | Consumo del mensaje | RabbitMQ | ✅ Cumple |
| CP-04 | Publicación de evento | Kafka | ✅ Cumple |
| CP-05 | Consumo del evento (fan-out) | Kafka | ✅ Cumple |
| CP-06 | Trazabilidad del flujo | Ambas | ✅ Cumple |
| CP-07 | Responsive | Ambas | ✅ Cumple |
| CP-08 | Message-driven | Ambas | ✅ Cumple |
| CP-09 | Resilient | Ambas | ✅ Cumple |
| CP-10 | Elastic | Ambas | ✅ Cumple |
| CP-11 | Desacoplamiento temporal | Ambas | ✅ Cumple |
| CP-12 | Recuperación tras caída | Ambas | ✅ Cumple |

---

### CP-01 — Creación de pedido vía API (RabbitMQ)

| Campo | Detalle |
|---|---|
| **ID** | CP-01 |
| **Arquitectura** | RabbitMQ |
| **Objetivo** | Verificar que la API recibe pedidos válidos y responde con HTTP 202. |
| **Precondiciones** | Stack de RabbitMQ levantado y en estado healthy. |
| **Datos de entrada** | `POST /orders` con `{ "customerName":"Pedro", "product":"Libro de arquitectura", "quantity":1 }` |
| **Pasos ejecutados** | 1. Levantamos `rabbitmq-poc` con `docker compose up -d`.<br>2. Mandamos el POST desde Postman.<br>3. Revisamos el código de respuesta y el cuerpo. |
| **Resultado esperado** | HTTP 202 con `status: accepted`, un `message` y un `orderId` no vacío. |
| **Resultado obtenido** | HTTP 202 con `orderId: "ORD-1778367297692"`, como se ve en la Figura 3. |

![Postman con la respuesta 202 y el orderId](evidencias/rabbitmq/02-postman-request.png)

*Figura 3. Postman mostrando la respuesta HTTP 202 y el `orderId` generado por la API.*

| Campo | Detalle |
|---|---|
| **Estado** | ✅ Cumple |
| **Análisis técnico** | El handler `app.post('/orders')` revisa que vengan los campos requeridos, genera un `orderId` único usando `Date.now()` y publica el mensaje en la cola sin esperar nada. La respuesta 202 sale de inmediato, así el cliente queda libre para seguir haciendo lo suyo. |

---

### CP-02 — Envío de mensaje a la cola (RabbitMQ)

| Campo | Detalle |
|---|---|
| **ID** | CP-02 |
| **Arquitectura** | RabbitMQ |
| **Objetivo** | Confirmar que cada POST genera un mensaje persistente en la cola `pedidos`. |
| **Precondiciones** | API conectada a RabbitMQ y la cola `pedidos` declarada como `durable: true`. |
| **Datos de entrada** | 3 pedidos con clientes Pedro, María y Carlos. |
| **Pasos ejecutados** | 1. Mandamos los 3 POST.<br>2. Abrimos la consola de RabbitMQ Management en http://localhost:15672.<br>3. Inspeccionamos la cola `pedidos`.<br>4. Revisamos los logs de la API. |
| **Resultado esperado** | La cola `pedidos` muestra los mensajes (en Ready o Total) y la API loguea `Mensaje publicado en cola "pedidos": ORD-...` por cada uno. |
| **Resultado obtenido** | Los 3 mensajes aparecieron publicados con sus `orderId` en los logs (Figura 4) y visibles en la consola de RabbitMQ (Figura 5). |

![Logs de orders-api publicando los 3 mensajes](evidencias/rabbitmq/04-logs-orders-api.png)

*Figura 4. Logs de `orders-api` mostrando los 3 mensajes publicados con sus respectivos `orderId`.*

![Cola pedidos con 3 mensajes en RabbitMQ Management](evidencias/rabbitmq/03-rabbitmq-cola-pedidos.png)

*Figura 5. Consola RabbitMQ Management mostrando la cola `pedidos` con los 3 mensajes encolados.*

| Campo | Detalle |
|---|---|
| **Estado** | ✅ Cumple |
| **Análisis técnico** | La API usa `channel.sendToQueue(queue, buffer, { persistent: true })`. Esa opción `persistent` marca el mensaje como durable, lo que significa que sobrevive si el broker se reinicia — algo importante en escenarios de producción donde no quieres perder pedidos. |

---

### CP-03 — Consumo del mensaje (RabbitMQ)

| Campo | Detalle |
|---|---|
| **ID** | CP-03 |
| **Arquitectura** | RabbitMQ |
| **Objetivo** | Validar que `notification-worker` consume y reconoce (con ack) cada mensaje de la cola. |
| **Precondiciones** | Worker corriendo y mensajes en la cola. |
| **Datos de entrada** | Los 3 mensajes que publicamos en CP-02. |
| **Pasos ejecutados** | 1. Corrimos `docker compose logs notification-worker`.<br>2. Buscamos las líneas de `Mensaje recibido` y `Notificación enviada`.<br>3. Confirmamos en la consola de RabbitMQ que la cola quedó en 0 Ready. |
| **Resultado esperado** | El worker procesa los 3 mensajes en orden, simula el envío de notificación con un delay de 2 segundos, y la cola termina vacía. |
| **Resultado obtenido** | El worker procesó Pedro → María → Carlos en orden y la cola quedó en 0, como se ve en los logs (Figura 6). |

![Logs del worker procesando los 3 mensajes](evidencias/rabbitmq/05-logs-worker.png)

*Figura 6. Logs del `notification-worker` procesando los 3 mensajes de la cola en orden, con su simulación y ack.*

| Campo | Detalle |
|---|---|
| **Estado** | ✅ Cumple |
| **Análisis técnico** | El consumidor usa `channel.prefetch(1)` para tomar solo un mensaje a la vez, y hace `channel.ack(msg)` después de simular el envío. Esto es importante: si el worker se cae justo antes del ack, RabbitMQ asume que el mensaje no se procesó y lo vuelve a entregar. Es una garantía de "al menos una vez". |

---

### CP-04 — Publicación de evento (Kafka)

| Campo | Detalle |
|---|---|
| **ID** | CP-04 |
| **Arquitectura** | Kafka |
| **Objetivo** | Verificar que un POST genera un evento `OrderCreated` con un esquema rico en el tópico `orders.events`. |
| **Precondiciones** | Kafka, Kafka UI y `orders-api` levantados. |
| **Datos de entrada** | `POST /orders` con `{ "customerName":"Pedro", "product":"Libro de arquitectura", "quantity":1, "unitPrice":85000 }` |
| **Pasos ejecutados** | 1. Mandamos el POST desde Postman.<br>2. Validamos respuesta 202.<br>3. En Kafka UI abrimos el tópico `orders.events`.<br>4. Inspeccionamos el JSON del evento. |
| **Resultado esperado** | Un evento JSON en el tópico con `eventId`, `eventType: OrderCreated`, `eventVersion`, `occurredAt`, `source`, `correlationId` y un `data` con el detalle del pedido. |
| **Resultado obtenido** | El POST devolvió 202 (Figura 7), el tópico `orders.events` aparece en Kafka UI con el contador de mensajes (Figura 8) y al abrir el mensaje vemos el evento completo con todos los metadatos esperados (Figura 9). |

![Postman con la respuesta 202 en Kafka POC](evidencias/kafka/13-postman-kafka.png)

*Figura 7. Postman con la respuesta 202 del endpoint `POST /orders` en la POC de Kafka.*

![Kafka UI con el tópico orders.events](evidencias/kafka/14-kafka-ui-topico.png)

*Figura 8. Kafka UI mostrando el tópico `orders.events` con su contador de mensajes y particiones.*

![Detalle del evento OrderCreated en Kafka UI](evidencias/kafka/15-kafka-ui-mensaje.png)

*Figura 9. Detalle del evento `OrderCreated` en Kafka UI: `eventId`, `correlationId`, `occurredAt` y el bloque `data` con el detalle del pedido.*

| Campo | Detalle |
|---|---|
| **Estado** | ✅ Cumple |
| **Análisis técnico** | A diferencia de lo que hicimos en RabbitMQ, acá el productor envía un **evento de dominio** (algo que ya pasó), no un comando (algo que pedimos hacer). El esquema incluye metadatos pensados para auditoría: `eventId` para identificar el evento de forma única, `correlationId` para trazar una operación completa entre servicios, una versión para evolucionar el contrato, y un timestamp. Esto es lo que hace que Kafka sea tan útil para event sourcing. |

---

### CP-05 — Consumo del evento (Kafka, fan-out por consumer groups)

| Campo | Detalle |
|---|---|
| **ID** | CP-05 |
| **Arquitectura** | Kafka |
| **Objetivo** | Verificar que los 3 consumidores (cada uno en un consumer group distinto) reciben **el mismo evento** en paralelo. |
| **Precondiciones** | Los 3 consumidores conectados con un `groupId` distinto. |
| **Datos de entrada** | El evento que publicamos en CP-04. |
| **Pasos ejecutados** | 1. `docker logs inventory-consumer`<br>2. `docker logs billing-consumer`<br>3. `docker logs notification-consumer`<br>4. Validamos en Kafka UI que aparezcan los 3 consumer groups. |
| **Resultado esperado** | Cada consumidor muestra `Evento recibido: OrderCreated`, el mismo `orderId`, su simulación específica, y el mismo `topic=orders.events partition=0 offset=0`. |
| **Resultado obtenido** | Los 3 procesaron el evento `ORD-1778374812807` y cada uno corrió su simulación específica. En Kafka UI se ven los 3 consumer groups separados (Figura 10), y en los logs (Figuras 11, 12 y 13) cada consumer muestra el mismo evento con su acción simulada. |

![Kafka UI con los 3 consumer groups](evidencias/kafka/16-kafka-ui-consumer-groups.png)

*Figura 10. Kafka UI mostrando los 3 consumer groups: `inventory-service-group`, `billing-service-group` y `notification-service-group`.*

![Logs de inventory-consumer](evidencias/kafka/17-logs-inventory.png)

*Figura 11. Logs de `inventory-consumer` recibiendo el evento y simulando la reserva de inventario.*

![Logs de billing-consumer](evidencias/kafka/18-logs-billing.png)

*Figura 12. Logs de `billing-consumer` recibiendo el mismo evento y simulando la generación de factura.*

![Logs de notification-consumer](evidencias/kafka/19-logs-notification.png)

*Figura 13. Logs de `notification-consumer` recibiendo el mismo evento y simulando el envío de notificación al cliente.*

| Campo | Detalle |
|---|---|
| **Estado** | ✅ Cumple |
| **Análisis técnico** | Acá se ve la magia del modelo pub/sub de Kafka. Como cada consumidor pertenece a un grupo distinto, Kafka entrega el mismo offset a los 3 — cada grupo lleva su propia cuenta de hasta dónde leyó. Esto deja agregar nuevos servicios que reaccionen al mismo evento sin tocar los existentes ni la API. Si los hubiéramos puesto en el mismo grupo, Kafka habría repartido los eventos entre ellos (un evento → un solo consumidor del grupo). |

---

### CP-06 — Trazabilidad (Ambas)

| Campo | Detalle |
|---|---|
| **ID** | CP-06 |
| **Arquitectura** | Ambas |
| **Objetivo** | Confirmar que cada mensaje o evento se puede rastrear desde la API hasta el o los consumidores. |
| **Precondiciones** | Ambos stacks corriendo y logs disponibles. |
| **Datos de entrada** | Los pedidos del flujo feliz de cada POC. |
| **Pasos ejecutados** | 1. Comparamos el `orderId` en los logs de `orders-api` contra los del worker (RabbitMQ).<br>2. Comparamos el `orderId` en los logs de `orders-api` contra los 3 consumers (Kafka).<br>3. Validamos `correlationId` y `eventId` en Kafka UI. |
| **Resultado esperado** | El mismo `orderId` aparece tanto en el productor como en el consumidor. En Kafka, además, se preservan `eventId` y `correlationId`. |
| **Resultado obtenido** | Trazabilidad confirmada en ambas arquitecturas. En RabbitMQ el `orderId` que publica la API (Figura 4) coincide con el que procesa el worker (Figura 6). En Kafka, el `orderId` y el `eventId` que aparecen en el mensaje del tópico (Figura 9) son los mismos que vemos en los logs de los 3 consumers (Figuras 11, 12 y 13). |

> 💡 Las evidencias de este caso ya se mostraron en CP-02, CP-03, CP-04 y CP-05. Aquí solo correlacionamos los `orderId` entre ellas.

| Campo | Detalle |
|---|---|
| **Estado** | ✅ Cumple |
| **Análisis técnico** | Para auditoría, Kafka es claramente superior: cada evento tiene un offset que nunca cambia, los mensajes no se borran cuando alguien los consume, y un consumidor nuevo puede leer todo el histórico. RabbitMQ borra el mensaje en cuanto el worker hace ack, lo que hace difícil revisar después qué fue lo que pasó. Para QA esto es importante porque permite reproducir bugs viendo el evento original. |

---

### CP-07 — Responsive (Ambas)

| Campo | Detalle |
|---|---|
| **ID** | CP-07 |
| **Arquitectura** | Ambas |
| **Objetivo** | Demostrar que la API responde de inmediato al cliente, sin esperar a que el consumidor termine. |
| **Precondiciones** | Stacks levantados. |
| **Datos de entrada** | `POST /orders` en ambas POCs. |
| **Pasos ejecutados** | 1. Medimos el tiempo de respuesta del POST en Postman.<br>2. Verificamos que llegara la respuesta 202 antes de que el consumidor procesara.<br>3. Comparamos el timestamp del log de la API contra el del log del consumer. |
| **Resultado esperado** | Respuesta por debajo de 100 ms, código 202, sin esperar a que el consumer termine su trabajo. |
| **Resultado obtenido** | RabbitMQ: ~17 ms (Figura 3). Kafka: ~17 ms (Figura 7, donde se ve "17ms" en la barra de Postman). En los dos casos la API respondió antes de que el consumer terminara — la simulación del worker tarda 2 segundos, y eso se ve claro en la diferencia de timestamps de los logs. |

> 💡 Las capturas de Postman ya se mostraron en CP-01 (Figura 3, RabbitMQ) y CP-04 (Figura 7, Kafka). En ambas se ve el tiempo de respuesta en la esquina superior derecha de Postman.

| Campo | Detalle |
|---|---|
| **Estado** | ✅ Cumple |
| **Análisis técnico** | El principio Responsive del Manifiesto Reactivo dice que el sistema debe responder en tiempos consistentes incluso bajo carga o fallo. Usar un broker logra justo eso: el tiempo de respuesta de la API ya no depende de cuánto tarde el procesamiento real. La API solo publica el mensaje y se libera. |

---

### CP-08 — Message-driven (Ambas)

| Campo | Detalle |
|---|---|
| **ID** | CP-08 |
| **Arquitectura** | Ambas |
| **Objetivo** | Probar que productor y consumidores se comunican exclusivamente vía broker, sin llamadas directas (HTTP/RPC). |
| **Precondiciones** | Acceso al código fuente. |
| **Datos de entrada** | N/A (revisión de código estático). |
| **Pasos ejecutados** | 1. Revisamos el handler POST de cada `orders-api`.<br>2. Verificamos que solo invoca `channel.sendToQueue()` en RabbitMQ y `producer.send()` en Kafka.<br>3. Hicimos `grep` de `axios`, `fetch` y `http.request` en los 3 consumers de Kafka y en el worker de RabbitMQ — 0 coincidencias.<br>4. Confirmamos que los `package.json` de los consumidores no incluyen `express` ni ningún cliente HTTP. |
| **Resultado esperado** | El productor solo publica al broker, el consumidor solo escucha del broker. Cero comunicación HTTP entre ellos. |
| **Resultado obtenido** | Confirmado. La API no importa ni instancia ningún cliente HTTP hacia los consumidores (Figuras 14 y 16). Los consumidores no exponen ningún endpoint y no usan Express (Figuras 15 y 17). El único punto de contacto entre productor y consumidor es el broker. |

![Código de orders-api en RabbitMQ](evidencias/rabbitmq/10-codigo-api-no-llama-worker.png)

*Figura 14. Código de `orders-api` en RabbitMQ. Solo invoca `channel.sendToQueue()`, sin clientes HTTP hacia el worker.*

![Código del notification-worker sin endpoints](evidencias/rabbitmq/11-codigo-worker-sin-endpoints.png)

*Figura 15. Código del `notification-worker`: no importa Express ni expone endpoints, solo se suscribe a la cola.*

![Código de orders-api en Kafka](evidencias/kafka/25-codigo-api-kafka.png)

*Figura 16. Código de `orders-api` en Kafka. Solo invoca `producer.send()`, sin clientes HTTP hacia los consumidores.*

![Código de un consumer de Kafka sin endpoints](evidencias/kafka/26-codigo-consumer-sin-http.png)

*Figura 17. Código de un consumer de Kafka: no importa Express ni expone endpoints, solo se suscribe al tópico.*

| Campo | Detalle |
|---|---|
| **Estado** | ✅ Cumple |
| **Análisis técnico** | El acoplamiento entre productor y consumidor queda únicamente en el contrato del mensaje o evento. Esto permite reemplazar, escalar o agregar consumidores sin tocar la API. Es lo que diferencia una arquitectura message-driven de una orientada a servicios con HTTP — acá nadie sabe quién está al otro lado. |

---

### CP-09 — Resilient (Ambas)

| Campo | Detalle |
|---|---|
| **ID** | CP-09 |
| **Arquitectura** | Ambas |
| **Objetivo** | Probar que el sistema sigue aceptando peticiones aunque un consumidor se caiga, y que los mensajes o eventos no se pierden. |
| **Precondiciones** | Stacks levantados. |
| **Datos de entrada** | 3 pedidos enviados con el consumidor apagado en cada POC. |
| **Pasos ejecutados** | 1. RabbitMQ: `docker compose stop notification-worker`, mandamos 3 POST y verificamos la acumulación.<br>2. Kafka: `docker stop inventory-consumer`, mandamos 3 POST, verificamos que los eventos quedaran en el log y que `billing` y `notification` sí los procesaran. |
| **Resultado esperado** | RabbitMQ: 3 mensajes en Ready (cola acumulada). Kafka: 3 nuevos offsets en el tópico, billing y notification los procesan, inventory queda atrás pero los recupera al reiniciar. |
| **Resultado obtenido — RabbitMQ** | La cola quedó con 3 mensajes en Ready y 0 consumers (Figura 18). Al reiniciar el worker, procesó los 3 mensajes acumulados (Figura 19). |

![Cola con 3 mensajes en Ready y worker apagado](evidencias/rabbitmq/06-worker-apagado-cola-acumulada.png)

*Figura 18. Cola `pedidos` con 3 mensajes acumulados en Ready y 0 consumers, mientras el worker está apagado.*

![Worker recuperado procesando los 3 mensajes acumulados](evidencias/rabbitmq/07-worker-recuperado-procesa.png)

*Figura 19. Logs del `notification-worker` después de reiniciarlo: procesa de inmediato los 3 mensajes acumulados.*

| Campo | Detalle |
|---|---|
| **Resultado obtenido — Kafka** | El consumer apagado se ve en `docker compose ps` (Figura 20). El tópico mostró 5 mensajes acumulados (los 2 originales del flujo feliz más los 3 nuevos enviados con inventory apagado, Figura 21); billing y notification procesaron los 3 nuevos en tiempo real, e inventory recuperó esos 3 al volver a arrancar (Figura 22). |

![inventory-consumer detenido](evidencias/kafka/20-inventory-apagado.png)

*Figura 20. `docker compose ps` con `inventory-consumer` detenido mientras el resto del stack sigue corriendo.*

![Eventos acumulados en el tópico](evidencias/kafka/21-eventos-en-topico.png)

*Figura 21. Kafka UI mostrando los 5 eventos acumulados en `orders.events` (2 del flujo feliz + 3 enviados con inventory apagado).*

![inventory-consumer recuperando los 3 eventos al reiniciar](evidencias/kafka/22-inventory-recupera.png)

*Figura 22. Logs de `inventory-consumer` al reiniciarlo: lee desde el último offset comprometido y procesa los 3 eventos acumulados.*

| Campo | Detalle |
|---|---|
| **Estado** | ✅ Cumple |
| **Análisis técnico** | Los dos brokers cumplen el principio, pero la forma de hacerlo es muy distinta. RabbitMQ entrega y olvida — los mensajes existen mientras no haya ack y desaparecen apenas el consumer confirma. Kafka conserva los eventos por un tiempo de retención configurable, lo que significa que un consumer nuevo puede consumir incluso eventos viejos. Para resilencia básica, ambos sirven; para auditoría e historicidad, Kafka tiene la ventaja. |

---

### CP-10 — Elastic (Ambas)

| Campo | Detalle |
|---|---|
| **ID** | CP-10 |
| **Arquitectura** | Ambas |
| **Objetivo** | Verificar el comportamiento al escalar consumidores horizontalmente. |
| **Precondiciones** | Stacks levantados. |
| **Datos de entrada** | 6 pedidos enviados a un grupo de N consumidores. |
| **Pasos ejecutados** | 1. RabbitMQ: `--scale notification-worker=3` y mandamos 6 POST.<br>2. Kafka: `--scale inventory-consumer=2` y mandamos 6 POST.<br>3. Comparamos los logs de cada réplica. |
| **Resultado esperado** | RabbitMQ: reparto round-robin entre los 3 workers. Kafka: distribución según el número de particiones del tópico. |
| **Resultado obtenido — RabbitMQ** | Los 3 workers se levantaron correctamente (Figura 23) y cada uno procesó 2 mensajes — reparto perfecto (Figura 24). |

![3 réplicas del notification-worker corriendo](evidencias/rabbitmq/08-multiples-workers-corriendo.png)

*Figura 23. Tres réplicas de `notification-worker` corriendo simultáneamente tras el `--scale notification-worker=3`.*

![Distribución de mensajes entre los 3 workers](evidencias/rabbitmq/09-distribucion-mensajes.png)

*Figura 24. Logs intercalados de los 3 workers: cada uno procesó 2 de los 6 mensajes (reparto round-robin).*

| Campo | Detalle |
|---|---|
| **Resultado obtenido — Kafka** | Las 2 réplicas se levantaron (Figura 25), pero con una sola partición en el tópico, solo `inventory-consumer-2` recibió los 6 eventos; `inventory-consumer-1` quedó en standby sin asignación de partición (Figura 26). |

![2 réplicas de inventory-consumer corriendo](evidencias/kafka/23-multiples-inventory.png)

*Figura 25. Dos réplicas de `inventory-consumer` corriendo tras el `--scale inventory-consumer=2`.*

![Solo una réplica recibe los eventos en Kafka](evidencias/kafka/24-distribucion-en-grupo.png)

*Figura 26. Logs mostrando que solo `inventory-consumer-2` procesa los eventos; la otra réplica queda en standby (sin asignación de partición).*

| Campo | Detalle |
|---|---|
| **Estado** | ✅ Cumple |
| **Análisis técnico** | **Hallazgo importante:** la elasticidad horizontal en Kafka está limitada por el número de particiones del tópico. Si tenemos más consumidores que particiones dentro de un mismo grupo, los que sobran quedan idle como respaldo en caliente. Para escalar de verdad hay que aumentar las particiones del tópico. RabbitMQ no tiene este límite — el reparto se hace por consumidor activo, no por particiones, así que escala más fácil hasta cierto punto. Esto explica por qué muchos arquitectos eligen Kafka para flujos pesados con planificación previa de particiones, y RabbitMQ para casos más dinámicos. |

---

### CP-11 — Desacoplamiento temporal (Ambas)

| Campo | Detalle |
|---|---|
| **ID** | CP-11 |
| **Arquitectura** | Ambas |
| **Objetivo** | Probar que productor y consumidor no necesitan estar arriba al mismo tiempo. |
| **Precondiciones** | Stacks levantados. |
| **Datos de entrada** | Pedidos enviados con el consumidor caído (mismo escenario que CP-09). |
| **Pasos ejecutados** | 1. Apagamos el consumidor.<br>2. Mandamos los pedidos.<br>3. Confirmamos que la API respondiera 202 sin error.<br>4. Reiniciamos el consumidor y observamos que procesara lo acumulado. |
| **Resultado esperado** | La API funciona, los mensajes/eventos quedan en el broker, el consumidor los procesa al volver. |
| **Resultado obtenido** | Confirmado en las dos arquitecturas. La evidencia es la misma de CP-09: en RabbitMQ los mensajes acumulados (Figura 18) se procesan al reiniciar el worker (Figura 19); en Kafka los eventos acumulados en el tópico (Figura 21) se procesan cuando inventory vuelve (Figura 22). |

> 💡 Este caso comparte evidencias con CP-09 porque ambos validan dos caras del mismo escenario: CP-09 mira la resiliencia del sistema (no se cae aunque falte un consumidor), CP-11 mira el desacoplamiento en el tiempo (no necesitan estar arriba al mismo tiempo).

| Campo | Detalle |
|---|---|
| **Estado** | ✅ Cumple |
| **Análisis técnico** | El desacoplamiento temporal es la base de todo modelo message-driven. El broker actúa como un buffer entre dominios — un servicio caído no propaga el fallo al productor, simplemente acumula trabajo pendiente. Esta es una de las razones por las que estos brokers se usan tanto en arquitecturas de microservicios. |

---

### CP-12 — Recuperación tras caída (Ambas)

| Campo | Detalle |
|---|---|
| **ID** | CP-12 |
| **Arquitectura** | Ambas |
| **Objetivo** | Verificar que un consumidor recuperado procesa el backlog acumulado durante su caída. |
| **Precondiciones** | Stacks con backlog acumulado. |
| **Datos de entrada** | Cola/tópico con N mensajes/eventos pendientes. |
| **Pasos ejecutados** | 1. Reiniciamos el consumer.<br>2. Observamos los logs.<br>3. Verificamos que se procesaran los mensajes acumulados, no solo los nuevos. |
| **Resultado esperado** | El consumer recuperado procesa el backlog completo. |
| **Resultado obtenido** | RabbitMQ: el worker reiniciado procesa los 3 mensajes acumulados (Figura 19). Kafka: inventory reiniciado lee desde el último offset comprometido y procesa los 3 eventos nuevos (offsets 2, 3 y 4, Figura 22). |

> 💡 Las evidencias de recuperación ya se mostraron en CP-09 (Figuras 19 y 22). La diferencia conceptual con CP-09 es el foco: CP-09 valida que el sistema no se cae, CP-12 valida que el consumer recupera específicamente el backlog acumulado durante su caída.

| Campo | Detalle |
|---|---|
| **Estado** | ✅ Cumple |
| **Análisis técnico** | El mecanismo es distinto en cada uno. RabbitMQ se basa en que los mensajes nunca tuvieron ack — al reiniciar el worker, simplemente se vuelven a entregar. Kafka usa offsets comprometidos por consumer group: el consumer recuerda exactamente en qué punto del log quedó y retoma desde ahí. **Para QA, Kafka es más auditable** porque podemos ver el offset comprometido en cualquier momento desde Kafka UI y saber con precisión cuánto le falta a cada consumer por procesar (lag). |

---

## 4. Análisis comparativo

Después de tener las dos arquitecturas funcionando y haberlas estresado con las pruebas reactivas, estas son las diferencias prácticas que sacamos:

| Aspecto | RabbitMQ | Kafka |
|---|---|---|
| Modelo | Cola tradicional (smart broker, dumb consumer) | Log distribuido (dumb broker, smart consumer) |
| Persistencia | Hasta el ack del consumer | Configurable por retención (por defecto, días) |
| Reparto entre consumidores | Round-robin entre los activos | Por particiones (1 partición → 1 consumer activo en el grupo) |
| Pub/Sub multi-servicio | Hay que armar exchanges (fanout/topic) | Listo de fábrica vía consumer groups distintos |
| Auditabilidad | Limitada — el mensaje desaparece tras ack | Excelente — offsets, retención, consumer lag |
| Lectura histórica | No (los mensajes se borran) | Sí (`fromBeginning: true` o seek a un offset) |
| Complejidad operacional | Baja (consola web simple y directa) | Media (KRaft o ZooKeeper, planificación de particiones) |
| Curva de aprendizaje para QA | Más natural si vienes de pruebas tradicionales | Toca aprender offsets, particiones y groups |

---

## 5. Conclusiones

### 5.1 ¿Qué arquitectura es más robusta desde QA?

Las dos cumplen los principios reactivos básicos sin problema, pero **desde la perspectiva de QA y auditoría, Kafka tiene la ventaja**. Como los eventos quedan en el log durante todo el período de retención, podemos volver a ejecutar pruebas, reproducir bugs que pasaron en producción y validar trazabilidad sin tener que recrear escenarios desde cero. La consola Kafka UI nos da offsets, lag por consumer group y el contenido completo del evento de un vistazo — esos son datos que en RabbitMQ se pierden apenas el worker hace ack.

Eso no quiere decir que RabbitMQ sea inferior. Es más simple de operar, la consola Management es más directa para QA tradicional (cuántos en Ready, cuántos consumers, cuántos sin ack), y para equipos pequeños o casos donde no necesitas histórico, es más liviano. La elección depende del problema: para sistemas con auditoría regulatoria o necesidad de event sourcing, Kafka encaja mejor; para colas de trabajo simples, RabbitMQ basta.

### 5.2 ¿Qué dificultades se presentaron en pruebas?

Algunas se las llevamos como aprendizaje del taller:

1. **La imagen de Bitnami estaba descontinuada.** `bitnami/kafka:3.7` ya no existe en Docker Hub — Bitnami movió sus imágenes legacy a un namespace distinto. La reemplazamos por `bitnamilegacy/kafka:3.7` y todo siguió funcionando igual.
2. **La configuración de listeners en Kafka es tramposa.** Kafka exige diferenciar `PLAINTEXT` (la red interna de Docker) de `EXTERNAL` (el acceso desde el host). Si configuras mal `KAFKA_CFG_ADVERTISED_LISTENERS`, los clientes no logran conectarse aunque los puertos estén abiertos. Nos tomó un rato entender por qué fallaba.
3. **Race condition en el primer arranque de los consumers de Kafka.** Si un consumer intenta suscribirse antes de que el tópico exista, falla con `UNKNOWN_TOPIC_OR_PARTITION`. Nos pasó con `notification-consumer` la primera vez. Bastó reiniciarlo una vez que `orders-api` creó el tópico al primer publish.
4. **El `container_name` impide escalar.** Docker Compose obliga a que los nombres de contenedor sean únicos, así que tuvimos que quitar el `container_name` del worker en RabbitMQ y del `inventory-consumer` en Kafka para poder usar `--scale` y crear réplicas.
5. **Una partición no se reparte entre dos consumers.** Intuitivamente uno espera que dos consumers escalados se repartan los mensajes, pero Kafka asigna particiones, no mensajes — y si el tópico tiene una sola, solo uno trabaja y el otro queda como respaldo. Esto fue justo el hallazgo de CP-10.

### 5.3 ¿Qué diferencias reales se evidenciaron?

Las pruebas dejaron muy claras estas diferencias:

1. **Persistencia.** RabbitMQ entrega y olvida; Kafka conserva los eventos durante el período de retención. Esto cambia completamente cómo abordas la auditoría.
2. **Reparto al escalar.** RabbitMQ repartió 6 mensajes entre 3 workers (2-2-2). Kafka, con una sola partición, le dio todo a un solo consumer y dejó al otro en standby.
3. **Modelo pub/sub.** En RabbitMQ hay que armar exchanges para hacer fan-out a múltiples servicios. En Kafka es nativo: un evento se publica, varios consumer groups lo procesan cada uno una vez.
4. **Lectura histórica.** En Kafka un consumer nuevo puede consumir desde el inicio del tópico (`fromBeginning: true`); en RabbitMQ solo recibe mensajes a partir de su conexión.
5. **El payload.** En RabbitMQ enviamos un mensaje tipo comando (`OrderCreatedMessage`); en Kafka diseñamos un evento de dominio rico, con `eventId`, `correlationId`, `version` y `occurredAt`. Esto refleja la filosofía de cada broker: comandos vs eventos. RabbitMQ está pensado para "haz esto"; Kafka para "esto pasó".

---

## 6. Anexo: archivos de configuración

### 6.1 `rabbitmq-poc/docker-compose.yml`

```yaml
services:
  rabbitmq:
    image: rabbitmq:3.13-management
    container_name: rabbitmq
    ports:
      - "5672:5672"
      - "15672:15672"
    environment:
      - RABBITMQ_DEFAULT_USER=admin
      - RABBITMQ_DEFAULT_PASS=admin
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5

  orders-api:
    build: ./orders-api
    container_name: orders-api
    ports:
      - "3000:3000"
    environment:
      - RABBITMQ_URL=amqp://admin:admin@rabbitmq:5672
      - QUEUE_NAME=pedidos
      - PORT=3000
    depends_on:
      rabbitmq:
        condition: service_healthy

  notification-worker:
    build: ./notification-worker
    environment:
      - RABBITMQ_URL=amqp://admin:admin@rabbitmq:5672
      - QUEUE_NAME=pedidos
    depends_on:
      rabbitmq:
        condition: service_healthy
```

> 📝 El servicio `notification-worker` no tiene `container_name` a propósito, para poder hacer `docker compose up --scale notification-worker=N` (CP-10). Si le pusiéramos un nombre fijo, Docker rechazaría crear las réplicas porque los nombres de contenedor deben ser únicos.

### 6.2 `kafka-poc/docker-compose.yml`

```yaml
services:
  kafka:
    image: bitnamilegacy/kafka:3.7
    container_name: kafka
    ports:
      - "29092:29092"
    environment:
      - KAFKA_CFG_NODE_ID=1
      - KAFKA_CFG_PROCESS_ROLES=broker,controller
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@kafka:9093
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,EXTERNAL://:29092,CONTROLLER://:9093
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092,EXTERNAL://localhost:29092
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - ALLOW_PLAINTEXT_LISTENER=yes

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    ports:
      - "8080:8080"
    environment:
      - KAFKA_CLUSTERS_0_NAME=local
      - KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka:9092
    depends_on:
      - kafka

  orders-api:
    build: ./orders-api
    container_name: orders-api
    ports:
      - "3000:3000"
    environment:
      - KAFKA_BROKER=kafka:9092
      - KAFKA_TOPIC=orders.events
      - PORT=3000
    depends_on:
      - kafka

  inventory-consumer:
    build: ./inventory-consumer
    environment:
      - KAFKA_BROKER=kafka:9092
      - KAFKA_TOPIC=orders.events
      - KAFKA_GROUP_ID=inventory-service-group
      - CONSUMER_NAME=inventory-consumer
    depends_on:
      - kafka

  billing-consumer:
    build: ./billing-consumer
    container_name: billing-consumer
    environment:
      - KAFKA_BROKER=kafka:9092
      - KAFKA_TOPIC=orders.events
      - KAFKA_GROUP_ID=billing-service-group
      - CONSUMER_NAME=billing-consumer
    depends_on:
      - kafka

  notification-consumer:
    build: ./notification-consumer
    container_name: notification-consumer
    environment:
      - KAFKA_BROKER=kafka:9092
      - KAFKA_TOPIC=orders.events
      - KAFKA_GROUP_ID=notification-service-group
      - CONSUMER_NAME=notification-consumer
    depends_on:
      - kafka
```

> 📝 Notas técnicas:
> - **Imagen:** usamos `bitnamilegacy/kafka:3.7` (no `bitnami/kafka:3.7`, que ya no está en el registro oficial de Bitnami).
> - **Modo KRaft:** este compose usa Kafka 3.7 en modo KRaft (`PROCESS_ROLES=broker,controller`), sin ZooKeeper. Es lo recomendado para versiones modernas.
> - **Listeners duales:** `PLAINTEXT` para comunicación interna entre contenedores (`kafka:9092`) y `EXTERNAL` para acceso desde el host (`localhost:29092`).
> - **`inventory-consumer` sin `container_name`:** lo dejamos así para poder escalarlo con `--scale` (ver CP-10).
