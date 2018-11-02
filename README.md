# Proto-Beam

Warning this is an evolving product, even **Pre-Alpha** quality. Don't use
this in production just yet.

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
- Annotation support for
    - Field description
    - Field deprecation
    
### Why ProtoBuf

- ProtoBuf has a good DSL, with contract first in mind.
- ProtoBuf has a binary versioning strategy: A message can even be decoded
  without a schema.
- ProtoBuf has a collection of Well Known Types: Native timestamps.
- ProtoBuf Java implementation has a powerfull schema API.
- ProtoBuf has options so we can change the behaviour of the pipeline.
- ProtoBuf has good tooling for generating cross language code.

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
- Register standard Options
