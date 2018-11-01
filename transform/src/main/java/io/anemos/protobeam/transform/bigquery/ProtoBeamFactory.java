package io.anemos.protobeam.transform.bigquery;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.SchemaProtoToBigQueryModel;

public class ProtoBeamFactory<T extends Message> {

    private Class<T> messageClass;
    private Descriptors.Descriptor descriptor;

    public ProtoBeamFactory(Class<T> messageClass, Descriptors.Descriptor descriptor) {
        this.messageClass = messageClass;
        this.descriptor = descriptor;
    }

    public BigQueryWriteFormatFunction<T> getBigQueryWriteFormatFunction() {
        return new BigQueryWriteFormatFunction<>(messageClass);
    }

    public TableSchema getBigQuerySchema() {
        return new SchemaProtoToBigQueryModel().getSchema(descriptor);
    }

}
