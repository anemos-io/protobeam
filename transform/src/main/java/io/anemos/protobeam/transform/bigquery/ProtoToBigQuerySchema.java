package io.anemos.protobeam.transform.bigquery;

import java.util.List;

/*
#standardSQL
CREATE TABLE `quantum-data-lake.calvin.batch_session_all`
(
  event_timestamp	TIMESTAMP,
  event_id	STRING,
  event_order	  INT64,
  channel	STRING,
  event_type	STRING,
  event_code	STRING,
  scope	STRUCT<
    page	STRING,
    overlay	STRING,
    `campaign`	STRING,
    `product`	STRING,
    `article`	STRING,
    `cart`	STRING,
    `order` STRING
  >,
  source	STRING,
  member STRUCT<
    id	STRING,
    locale	STRING
  >,
  device	STRING,
  device_session	STRING,
  member_session	STRING,
  client_info	STRUCT<
  ip	STRING,
  device	STRUCT<
    class	STRING,
    name	STRING,
    brand	STRING
  >,
  os	STRUCT<
    class	STRING,
    name	STRING,
    version	STRING,
    build	STRING
  >,
  layout_engine	STRUCT<
    class	STRING,
    name	STRING,
    major_version	STRING,
    version	STRING
  >,
  agent	STRUCT<
    class	STRING,
    name	STRING,
    major_version	STRING,
    version	STRING
  >,
  viewport STRUCT<
    width	  INT64,
    height	  INT64
  >,
  locale	STRING
  >,
  extra_data ARRAY<STRUCT<
  key	STRING,
  value	STRING
  >>
)
PARTITION BY DATE(`event_timestamp`)

 */
public class ProtoToBigQuerySchema {




    public List<String> materialize() {
        return null;
    }
}
