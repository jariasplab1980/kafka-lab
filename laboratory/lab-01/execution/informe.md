# ![lab-01](../../../img/banner/01.png)

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
broker1:19092 (id: 1 rack: null) -> 
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
Topic: equipo10.lab01.facturacion.emitida       TopicId: STnB_w6FS36hvq3MD8sz3g PartitionCount: 3       ReplicationFactor: 3    Configs: min.insync.replicas=2,unclean.leader.election.enable=false
        Topic: equipo10.lab01.facturacion.emitida       Partition: 0    Leader: 3       Replicas: 3,1,2 Isr: 3,1,2      Elr:    LastKnownElr: 
        Topic: equipo10.lab01.facturacion.emitida       Partition: 1    Leader: 1       Replicas: 1,2,3 Isr: 1,2,3      Elr:    LastKnownElr: 
        Topic: equipo10.lab01.facturacion.emitida       Partition: 2    Leader: 2       Replicas: 2,3,1 Isr: 2,3,1      Elr:    LastKnownElr: 
```

### 3. Broker líder de la partición 0 antes de la caída.

**Resultado Obtenido**:  
```bash
estudiante@ip-192-168-166-62:/home/est2/kafka-dian/entorno$ docker exec broker3 ls -lh /var/lib/kafka/data/${TOPIC}-0/
total 12K
-rw-r--r-- 1 appuser appuser  10M Oct 13 04:39 00000000000000000000.index
-rw-r--r-- 1 appuser appuser 1.8K Oct 13 04:41 00000000000000000000.log
-rw-r--r-- 1 appuser appuser  10M Oct 13 04:39 00000000000000000000.timeindex
-rw-r--r-- 1 appuser appuser    8 Oct 13 04:39 leader-epoch-checkpoint
-rw-r--r-- 1 appuser appuser   43 Oct 13 04:39 partition.metadata
```


```bash
estudiante@ip-192-168-166-62:/home/est2/kafka-dian/entorno$ docker exec broker3 /opt/kafka/bin/kafka-dump-log.sh --print-data-log \
  --files /var/lib/kafka/data/${TOPIC}-0/00000000000000000000.log | head -20
Dumping /var/lib/kafka/data/equipo10.lab01.facturacion.emitida-0/00000000000000000000.log
Log starting offset: 0
baseOffset: 0 lastOffset: 35 count: 36 baseSequence: 0 lastSequence: 35 producerId: 2000 producerEpoch: 0 partitionLeaderEpoch: 0 isTransactional: false isControl: false deleteHorizonMs: OptionalLong.empty position: 0 CreateTime: 1791866483101 size: 1805 magic: 2 compresscodec: none crc: 757280239 isvalid: true
| offset: 0 CreateTime: 1791866483062 keySize: 4 valueSize: 35 sequence: 0 headerKeys: [] key: 9001 payload: {"nit":"9001","valor":170720,"n":1}
| offset: 1 CreateTime: 1791866483091 keySize: 4 valueSize: 35 sequence: 1 headerKeys: [] key: 9005 payload: {"nit":"9005","valor":113400,"n":5}
| offset: 2 CreateTime: 1791866483091 keySize: 5 valueSize: 36 sequence: 2 headerKeys: [] key: 90010 payload: {"nit":"90010","valor":72050,"n":10}
| offset: 3 CreateTime: 1791866483092 keySize: 5 valueSize: 35 sequence: 3 headerKeys: [] key: 90016 payload: {"nit":"90016","valor":6630,"n":16}
| offset: 4 CreateTime: 1791866483092 keySize: 5 valueSize: 37 sequence: 4 headerKeys: [] key: 90021 payload: {"nit":"90021","valor":261540,"n":21}
| offset: 5 CreateTime: 1791866483093 keySize: 5 valueSize: 36 sequence: 5 headerKeys: [] key: 90023 payload: {"nit":"90023","valor":87010,"n":23}
| offset: 6 CreateTime: 1791866483093 keySize: 5 valueSize: 37 sequence: 6 headerKeys: [] key: 90024 payload: {"nit":"90024","valor":171890,"n":24}
| offset: 7 CreateTime: 1791866483093 keySize: 5 valueSize: 36 sequence: 7 headerKeys: [] key: 90027 payload: {"nit":"90027","valor":34160,"n":27}
| offset: 8 CreateTime: 1791866483093 keySize: 5 valueSize: 36 sequence: 8 headerKeys: [] key: 90030 payload: {"nit":"90030","valor":57940,"n":30}
| offset: 9 CreateTime: 1791866483094 keySize: 5 valueSize: 37 sequence: 9 headerKeys: [] key: 90032 payload: {"nit":"90032","valor":320120,"n":32}
| offset: 10 CreateTime: 1791866483094 keySize: 5 valueSize: 36 sequence: 10 headerKeys: [] key: 90034 payload: {"nit":"90034","valor":68090,"n":34}
| offset: 11 CreateTime: 1791866483094 keySize: 5 valueSize: 37 sequence: 11 headerKeys: [] key: 90038 payload: {"nit":"90038","valor":214980,"n":38}
| offset: 12 CreateTime: 1791866483095 keySize: 5 valueSize: 37 sequence: 12 headerKeys: [] key: 90041 payload: {"nit":"90041","valor":173190,"n":41}
| offset: 13 CreateTime: 1791866483095 keySize: 5 valueSize: 36 sequence: 13 headerKeys: [] key: 90042 payload: {"nit":"90042","valor":26090,"n":42}
| offset: 14 CreateTime: 1791866483095 keySize: 5 valueSize: 37 sequence: 14 headerKeys: [] key: 90044 payload: {"nit":"90044","valor":194040,"n":44}
| offset: 15 CreateTime: 1791866483095 keySize: 5 valueSize: 37 sequence: 15 headerKeys: [] key: 90045 payload: {"nit":"90045","valor":253370,"n":45}
| offset: 16 CreateTime: 1791866483096 keySize: 5 valueSize: 37 sequence: 16 headerKeys: [] key: 90047 payload: {"nit":"90047","valor":248600,"n":47}
```

### 4. Salida después de detener el broker.

**Resultado Obtenido**:  

```bash
estudiante@ip-192-168-166-62:/home/est2/kafka-dian/entorno$ docker stop broker3
broker3
```

```bash
estudiante@ip-192-168-166-62:/home/est2/kafka-dian/entorno$ docker exec broker3 ls -lh /var/lib/kafka/data/${TOPIC}-0/
Error response from daemon: container db67a9419fa01de5ad5d91b78ea79729f9933005893238ffbba41de09fadf191 is not running
```

```bash
estudiante@ip-192-168-166-62:/home/est2/kafka-dian/entorno$ kt --describe --topic "$TOPIC"
Topic: equipo10.lab01.facturacion.emitida       TopicId: STnB_w6FS36hvq3MD8sz3g PartitionCount: 3       ReplicationFactor: 3    Configs: min.insync.replicas=2,unclean.leader.election.enable=false
        Topic: equipo10.lab01.facturacion.emitida       Partition: 0    Leader: 1       Replicas: 3,1,2 Isr: 1,2        Elr:    LastKnownElr: 
        Topic: equipo10.lab01.facturacion.emitida       Partition: 1    Leader: 1       Replicas: 1,2,3 Isr: 1,2        Elr:    LastKnownElr: 
        Topic: equipo10.lab01.facturacion.emitida       Partition: 2    Leader: 2       Replicas: 2,3,1 Isr: 2,1        Elr:    LastKnownElr: 
```

### 5. Salida después de reiniciar el broker.

**Resultado Obtenido**:  

```bash
estudiante@ip-192-168-166-62:/home/est2/kafka-dian/entorno$ kt --describe --topic "$TOPIC"
Topic: equipo10.lab01.facturacion.emitida       TopicId: STnB_w6FS36hvq3MD8sz3g PartitionCount: 3       ReplicationFactor: 3    Configs: min.insync.replicas=2,unclean.leader.election.enable=false
        Topic: equipo10.lab01.facturacion.emitida       Partition: 0    Leader: 1       Replicas: 3,1,2 Isr: 1,2,3      Elr:    LastKnownElr: 
        Topic: equipo10.lab01.facturacion.emitida       Partition: 1    Leader: 1       Replicas: 1,2,3 Isr: 1,2,3      Elr:    LastKnownElr: 
        Topic: equipo10.lab01.facturacion.emitida       Partition: 2    Leader: 2       Replicas: 2,3,1 Isr: 2,1,3      Elr:    LastKnownElr: 
```

### 6. Conteo final de mensajes.

**Resultado Obtenido**:  

```bash
estudiante@ip-192-168-166-62:/home/est2/kafka-dian/entorno$ docker exec broker3 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker1:19092   
--topic "$TOPIC" --from-beginning --timeout-ms 10000 2>/dev/null | wc -l
110
```

```bash
estudiante@ip-192-168-166-62:/home/est2/kafka-dian/entorno$ docker exec broker3 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker1:19092   
--topic "$TOPIC" --from-beginning --timeout-ms 10000 2>/dev/null | wc -l
110
```

### 7. Respuestas:

#### 7.1. ¿Qué archivos componen un segmento de Kafka?

```text
-rw-r--r-- 1 appuser appuser  10M Oct 13 04:39 00000000000000000000.index
-rw-r--r-- 1 appuser appuser 1.8K Oct 13 04:41 00000000000000000000.log
-rw-r--r-- 1 appuser appuser  10M Oct 13 04:39 00000000000000000000.timeindex
-rw-r--r-- 1 appuser appuser    8 Oct 13 04:39 leader-epoch-checkpoint
-rw-r--r-- 1 appuser appuser   43 Oct 13 04:39 partition.metadata
```

### 7.2. ¿Qué pasó con el ISR durante la caída?

El broker3 sigue siendo una réplica configurada de la partición, pero ya no forma parte del ISR porque dejó de estar sincronizado/disponible.

antes de:
```text
Topic: equipo10.lab01.facturacion.emitida       Partition: 0    Leader: 2       Replicas: 2,3,1 Isr: 3,1,2      Elr:    LastKnownElr: 
Topic: equipo10.lab01.facturacion.emitida       Partition: 1    Leader: 3       Replicas: 3,1,2 Isr: 3,1,2      Elr:    LastKnownElr: 
Topic: equipo10.lab01.facturacion.emitida       Partition: 2    Leader: 1       Replicas: 1,2,3 Isr: 3,1,2      Elr:    LastKnownElr: 
```
despues de
```text
Topic: equipo10.lab01.facturacion.emitida       Partition: 0    Leader: 3       Replicas: 3,1,2 Isr: 3,1,2      Elr:    LastKnownElr: 
Topic: equipo10.lab01.facturacion.emitida       Partition: 1    Leader: 1       Replicas: 1,2,3 Isr: 1,2,3      Elr:    LastKnownElr: 
Topic: equipo10.lab01.facturacion.emitida       Partition: 2    Leader: 2       Replicas: 2,3,1 Isr: 2,3,1      Elr:    LastKnownElr: 
```

### 7.3. ¿Por qué no se perdieron mensajes?

La partición tenía tres copias, cuando cayó broker3, todavía quedaron broker1 y broker2, copias sincronizadas permanecieron disponibles


### 7.4. ¿Qué riesgo habría con min.insync.replicas=1 y acks=1?

Prioriza disponibilidad/latencia, pero aumenta el riesgo de que un mensaje confirmado se pierda si el único broker que lo recibió falla antes de replicarlo.