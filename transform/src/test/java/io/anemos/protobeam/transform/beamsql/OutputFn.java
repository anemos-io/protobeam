package io.anemos.protobeam.transform.beamsql;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

public class OutputFn extends DoFn<Row, Void> {
    @DoFn.ProcessElement
    public void test(DoFn<Row, Void>.ProcessContext in) {


        System.out.println("PCOLLECTION: " + in.element());
    }

}

