---
sidebar_position: 4
---

# Produce Records

## JSON with schema producer

Run the following command (for json with schema producer)

```bash
python3 quickstart/producers/json-producer/producer.py --topic connect-demo --bootstrap-servers localhost:9093
```

> topic is mandatory

> Default value of bootstrap-servers is ```localhost:9092```

> Default value of schema-registry is ```http://localhost:8081```

> Default value of schema is ```../../schemas/json-schema.json```