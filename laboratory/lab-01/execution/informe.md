# Informe — Lab 01

Este directorio contiene las evidencias correspondientes a la ejecución del **Laboratorio 01**, desarrollado como parte del curso de Apache Kafka.

## Evidencias

### 1. Salida paso 5

Verificar brokers del clúster

```bash
docker exec broker1 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server broker1:19092 | grep -E "^broker[0-9]"
```

**Resultado Obtenido**:  

```bash
estudiante@ip-192-168-166-62:/home/est2/kafka-dian/entorno$ docker exec broker1 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server broker1:19092 | grep -E "^broker[0-9]"
broker3:19092 (id: 3 rack: null) -> (
broker2:19092 (id: 2 rack: null) -> (
broker1:19092 (id: 1 rack: null) -> (
```

### 2. Salida del paso 7

```bash
kt --describe --topic "$TOPIC"
```

Identifique para la partición 0:

Leader
Replicas
Isr

**Resultado Obtenido**:  

```bash
estudiante@ip-192-168-166-62:/home/est2/kafka-dian/entorno$ kt --describe --topic "$TOPIC"
Topic: equipo10.lab01.facturacion.emitida       TopicId: AKTR0LmpRtCBDpBXPjrGng PartitionCount: 3       ReplicationFactor: 3    Configs: min.insync.replicas=2,unclean.leader.election.enable=false
        Topic: equipo10.lab01.facturacion.emitida       Partition: 0    Leader: 2       Replicas: 2,3,1 Isr: 2,3,1      Elr:    LastKnownElr:
        Topic: equipo10.lab01.facturacion.emitida       Partition: 1    Leader: 3       Replicas: 3,1,2 Isr: 3,1,2      Elr:    LastKnownElr: 
        Topic: equipo10.lab01.facturacion.emitida       Partition: 2    Leader: 1       Replicas: 1,2,3 Isr: 1,2,3      Elr:    LastKnownElr: 
```
