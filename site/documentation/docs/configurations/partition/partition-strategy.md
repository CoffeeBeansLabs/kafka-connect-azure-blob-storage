---
sidebar_position: 1
---

# Partition strategy

Currently three types of partition strategies are supported by the connector.

These are: 
1. DEFAULT
2. FIELD
3. TIME

## Configuration

Key: `partition.strategy`

Value: Supported partition strategies mentioned above

Type: `String`

Optional: <span style={{color: "green"}}>Yes</span>

Default value: `DEFAULT`

```json
{...
    "partition.strategy": "STRATEGY"
}
```

> Importance: <span style={{color: "green"}}>LOW</span>