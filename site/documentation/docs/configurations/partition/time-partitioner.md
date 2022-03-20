---
sidebar_position: 3
---

# Time partitioner

Partition incoming records based on the timestamp.

## Timestamp extractor

Supported timestamp extractors are:

1. Default Timestamp Extractor
2. Record Timestamp Extractor
3. Record Field Timestamp Extractor

### Default timestamp extractor

This extractor uses the server date and time to build the path.

Key: `timestamp.extractor`

Value: `Default`

Type: `String`

```json
{...
    "timestamp.extractor": "Default"
}
```

> Importance: <span style={{color: "orange"}}>MEDIUM</span>

### Record timestamp extractor

This extractor uses the date and time at which the record was produced to kafka to build the path.

Key: `timestamp.extractor`

Value: `Record`

Type: `String`

```json
{...
    "timestamp.extractor": "Record"
}
```

> Importance: <span style={{color: "orange"}}>MEDIUM</span>

### RecordField timestamp extractor

This extractor extracts the timestamp from the field name specified in the `partition.strategy.time.field` to build the path

Key: `timestamp.extractor`

Value: `RecordField`

Type: `String`

```json
{...
    "timestamp.extractor": "RecordField"
}
```

> Importance: <span style={{color: "orange"}}>MEDIUM</span>


## Timestamp field

Specify the field name of the record from which the partitioner should extract the timestamp.

> Applicable only if the `timestamp.extractor` is set to `RecordField`

Key: `partition.strategy.time.field`

Value: `field_name`

Type: `String`

Optional: No

```json
{...
    "partition.strategy.time.field": "field_name"
}
```

> Importance: <span style={{color: "orange"}}>MEDIUM</span>


## Path Format

String path format into which the date and time values will be substituted.

Key: `path.format`

Example Value: `'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH/'zone'=z`

Type: `String`

Optional: No

```json
{...
    "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH/'zone'=z"
}
```

> Importance: <span style={{color: "orange"}}>MEDIUM</span>

## Timezone

Specify the timezone for the time partitioner

Key: `timezone`

Value: `Asia/Kolkata`

Type: `String`

Optional: Yes

Default: `UTC`

```json
{...
    "timezone": "Asia/Kolkata"
}
```

> Importance: <span style={{color: "orange"}}>MEDIUM</span>