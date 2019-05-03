package io.anemos.protobeam.transform.beamsql;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

public class ToStringFn extends DoFn<Row, String> {
  @DoFn.ProcessElement
  public void test(DoFn<Row, String>.ProcessContext in) {
    in.output(in.element().toString());
    System.out.println("PCOLLECTION: " + in.element());
  }
}
