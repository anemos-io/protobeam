package io.anemos.protobeam.convert.nodes.genericrecord;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractConvert;
import org.apache.avro.generic.GenericRecord;

abstract class AbstractGenericRecordConvert<T> extends AbstractConvert<T, GenericRecord, GenericRecord> {
    public AbstractGenericRecordConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public Object convert(Object in) {
        return null;
    }

    @Override
    public void convert(Message message, GenericRecord row) {

    }
}
