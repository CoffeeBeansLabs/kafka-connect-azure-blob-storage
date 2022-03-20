---
sidebar_position: 2
---

# Container

Configuration related to the container where the blobs will be created.

## Fixed container

The container name is passed with the connector configurations and it remains unchanged.

Key: `container.name`

Value: `container_name`

Type: `String`

Optional: <span style={{color: "green"}}>Yes</span>

Default value: `default`

```json
{...
    "container.name": "container_name"
}
```

The container name should be a valid string supported by azure blob storage.

> Importance: <span style={{color: "green"}}>LOW</span>
