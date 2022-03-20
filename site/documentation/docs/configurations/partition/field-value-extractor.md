---
sidebar_position: 2
---

# Field value extractor

> This configuration is only for <span style={{color: "red"}}>FIELD</span> based partitioning.

If partition is done based on field value then the name of that field should be passed to this configuration.

## Configuration

Key: `partition.strategy.field.name`

Value: `field_name`

Type: `String`

```json
{...
    "partition.strategy.field": "field_name"
}
```

> Importance: <span style={{color: "orange"}}>MEDIUM</span>