package io.anemos.protobeam.convert.nodes;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.avro.generic.GenericRecord;

public abstract class AbstractConvert<T> {
    protected Descriptors.FieldDescriptor descriptor;

    public AbstractConvert(Descriptors.FieldDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    public T convert(Object in) {
        return (T) in;
    }

    public abstract void convert(Message message, TableRow row);

    public abstract void convertToProto(Message.Builder builder, TableRow row);

    public abstract void convertToProto(Message.Builder builder, GenericRecord row);

    public T convertFromGenericRecord(Object in) {
        return (T) in;
    }

    public T convertFromTableCell(Object in) {
        return (T) in;
    }
}