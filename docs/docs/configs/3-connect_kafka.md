---
layout: default
---

# Connect Kafka 
Kafka is the most popular streaming engine today.
No matter you deployed your own cluster or you use confluent cloud hosted services. Nebula can connect to your topic for real time analytics.

Here is an example shown how to add a table config to connect a topic created in confluent cloud.
By default, it uses SASL_SSL secure protocol for the connection.
You need an account with API key and secret which configured as `sasl.username` and `sasl.password` respectively.
Also you need a DNS address to connect their brokers, usually with port 9092.
```yaml
  k.test1:
    retention:
      max-mb: 20000
      max-hr: 24
    schema: "ROW<name:string, value:int>"
    data: kafka
    topic: test2
    loader: Streaming
    source: <BROKER ADDRESS>
    backup: s3://nebula/n116/
    format: json
    columns:
      name:
        dict: true
    time:
      # kafka will inject a time column when specified provided
      type: provided
    settings:
      batch: 500
      kafka.sasl.mechanisms: "PLAIN"
      kafka.security.protocol: "SASL_SSL"
      kafka.sasl.username: "<API KEY>"
      kafka.sasl.password: "<API SECRET>"
```

put this section in cluster config and you should be ready to use it.