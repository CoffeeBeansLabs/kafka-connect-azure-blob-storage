---
sidebar_position: 1
---

# Connection

Configuration parameters for connecting to Azure blob storage service.

## Connection URL

A connection url which will be used by the connector to connect to the azure
blob storage service.

Key: `connection.url`

Value: `connection_url`

Type: `String`

Optional: <span style={{color: "red"}}>No</span>

```json
{...
    "connection.url": "connection_url"
}
```

If the connection url is malformed the connector will throw exception and will fail to start.

> Importance: <span style={{color: "red"}}>HIGH</span>