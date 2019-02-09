package io.anemos.protobuf.examples;

import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;

interface DirectTestOptions extends BigQueryOptions, DirectOptions {
}
