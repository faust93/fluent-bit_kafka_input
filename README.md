## Fluent Bit Kafka Input

More or less working kafka input plugin for Fluent Bit :)

Kafka input being promised for several years atm but nothing was done in that respect except from some recent half-assed PoC attempt
Though `in_kafka` is included in current codebase it does not work for me, it's slow and crashes FB quite requently

FB included PoC implementation uses threads and has no queue/buffering so consuming speed is quite slow
I rewrote it to use interval polling (`flb_input_set_collector_time`) instead of threads and added rudimentary queue/buffering

### Building
```bash
$ git clone -b 1.9.9-in_kafka https://github.com/faust93/fluent-bit_kafka_input.git
$ cd fluent-bit_kafka_input/build
$ cmake3 .. -DFLB_OUT_KAFKA=On -DFLB_IN_KAFKA=On -DFLB_JEMALLOC=On
$ make
```

### Config
```
...
[INPUT]
    Name               kafka
    brokers            localhost:9092
    topics             topic1,topic2,topic3
    Tag                kafka_input.*
    format             json
    rdkafka.group.id   fluentbit
    rdkafka.fetch.max.bytes  2024000
    Buffer_Chunk_Size  5MB
    Buffer_Size        128MB
    Mem_Buf_Limit      128MB

[FILTER]
    Name         nest
    Match        kafka_input.*
    Operation    lift
    Nested_under payload

[FILTER]
    Name       modify
    Match      kafka_input.*
    Remove     partition
    Remove     offset
    Remove     error
    Remove     key
...
```
