# Apache Kafka para la gestión de flujos de datos en tiempo real

> **Laboratorio de formación basado en Apache Kafka, KRaft y arquitecturas orientadas a eventos.**

PIRA — **Piedradura Impuestos y Recaudo Administrativo** — es un entorno académico y experimental diseñado para explorar la gestión de **flujos de datos en tiempo real utilizando Apache Kafka**.

El laboratorio simula una plataforma de intercambio de eventos, permitiendo trabajar con conceptos como:

* Event Streaming
* Topics y particiones
* Productores y consumidores
* Consumer Groups
* Replicación
* High Availability
* KRaft
* Retención de eventos
* Log Compaction
* ISR (*In-Sync Replicas*)
* Administración de clusters Kafka
* Observabilidad y operación de brokers

> 🦴 **Porque incluso en la Edad de Piedra los eventos tenían que llegar a tiempo.**

---

## Arquitectura

El laboratorio utiliza un cluster de **3 brokers Apache Kafka 3.9.0**, ejecutándose en modo **KRaft**, sin dependencia de ZooKeeper.

```text
                  ┌───────────────────────┐
                  │       Kafka UI        │
                  │       :8081           │
                  └───────────┬───────────┘
                              │
                              │ Internal
                              ▼
             ┌─────────────────────────────────┐
             │             PIRA Kafka Lab      │
             │                                 │
             │              KRaft Cluster      │
             │                                 │
             │  ┌────────────┐ ┌────────────┐  │
             │  │  Broker 1  │ │  Broker 2  │  │
             │  │ Node ID: 1 │ │ Node ID: 2 │  │
             │  │ :9092      │ │ :9094      │  │
             │  └─────┬──────┘ └─────┬──────┘  │
             │        │              │         │
             │        └───────┬──────┘         │
             │                │                │
             │         ┌──────▼──────┐         │
             │         │   Broker 3  │         │
             │         │ Node ID: 3  │         │
             │         │ :9096       │         │
             │         └─────────────┘         │
             └─────────────────────────────────┘
                              │
                              ▼
                     PIRA Event Streams
```

### Componentes

| Componente     | Tecnología            | Propósito                       |
| -------------- | --------------------- | ------------------------------- |
| Broker 1       | Apache Kafka 3.9.0    | Broker + Controller             |
| Broker 2       | Apache Kafka 3.9.0    | Broker + Controller             |
| Broker 3       | Apache Kafka 3.9.0    | Broker + Controller             |
| Cluster mode   | KRaft                 | Metadata management             |
| Administración | Kafka UI              | Administración y observabilidad |
| Topics         | Kafka                 | Flujos de eventos PIRA          |
| Persistencia   | Docker/Podman volumes | Datos Kafka                     |

---

## Inicio

### Requisitos

Se requiere:

* Docker Engine, Podman o Rancher Desktop
* Docker Compose compatible
* **8 GB de RAM libres**
* Puertos disponibles:

```text
8081
9092
9094
9096
9101
9102
9103
```

#### Entorno de formación

Este laboratorio está diseñado exclusivamente para **formación y experimentación**.

No utilizar con:

* Datos reales de contribuyentes
* Información tributaria real
* Información personal
* Credenciales reales
* Cargas productivas

El entorno inicial utiliza **PLAINTEXT**, sin cifrado ni autenticación.

La configuración de seguridad se aborda posteriormente en las sesiones correspondientes del curso.

---

## Levantar el laboratorio

Ejecutar:

```bash
docker compose -f docker-compose-lab-kafka.yml up -d
```

Verificar los servicios:

```bash
docker compose -f docker-compose-lab-kafka.yml ps
```

La salida esperada será similar a:

```text
NAME               STATUS
broker1            healthy
broker2            healthy
broker3            healthy
kafka-ui           running
bootstrap-topics   exited
```

El contenedor `bootstrap-topics` puede finalizar correctamente después de crear los topics.

---

## Detener el laboratorio

Para detener los contenedores:

```bash
docker compose -f docker-compose-lab-kafka.yml down
```

Para detenerlos y eliminar también los volúmenes:

```bash
docker compose -f docker-compose-lab-kafka.yml down -v
```

> `down -v` elimina la persistencia local del cluster.

Esto significa que los datos y metadatos almacenados en `data1`, `data2` y `data3` serán eliminados.

---

## Kafka UI

La interfaz de administración está disponible en:

```text
http://localhost:8081
```

El cluster aparece identificado como:

```text
pira-lab
```

Desde Kafka UI es posible explorar:

* Brokers
* Topics
* Particiones
* Consumer Groups
* Mensajes
* Configuraciones
* Replicas
