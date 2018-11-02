# Proto-Beam

## Description

Utilities for handling and converting ProtocolBuffers. Focused but not
limited to Apache Beam.

### Features

- Convert ProtoBuf schema to
    - BigQuery proto model
    - BigQuery api model
- Convert from ProtoBuf to
    - TableRow
- Convert to ProtoBuf from
    - TableRow
    - GenericRecord (BigQuery dump)
- Apache Beam Formatter for
    - BigQueryIO.TypedRead
    - BigQueryIO.Write

### Known Issues

- DynamicMessage doesn't seem to be supported by ProtoCodec (in Beam)
- Only tested happy flow
- OneOf not supported yet
- Everything in BigQuery is NULLABLE

## Examples

| env name | example |
| ------------- | -------------|
| RUNNER | Direct |
| GOOGLE_PROJECT_ID | myproject |
| BQ_GCS_TEMP | gs://my-bucket/tmp |
| BQ_OUT_DATASET | my-project:my-dataset |

Create 3 tables in a dataset, converted from Protobuf

[transform/src/main/java/ProtoToBigQueryPipeline]()

Read the created tables, convert to protobuf and output to stderr

[transform/src/main/java/ProtoFromBigQueryPipeline]()

## Roadmap

- ProtoBuf to BigQuery DDL
- ProtoBuf to Spanner
- ProtoBuf to/from BSON
- ProtoBuf to/from Avro
- Recursive messages
- OneOf type
- Well Known Types
- Wrapper Types
- Plugable message type convertions
- Options support for
    - Description
    - Deprecation
    - Flattening

