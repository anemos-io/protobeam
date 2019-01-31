package io.anemos.protobeam.convert.nodes.beamsql;

import com.google.protobuf.Descriptors;
import io.anemos.protobeam.convert.ConvertNodeFactory;
import io.anemos.protobeam.convert.nodes.AbstractConvert;
import io.anemos.protobeam.convert.nodes.MessageConvert;

import java.util.List;

public class BeamSqlConvertNodeFactory implements ConvertNodeFactory {
    @Override
    public AbstractConvert createBooleanFieldConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return null;
    }

    @Override
    public AbstractConvert createBytesFieldConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return null;
    }

    @Override
    public AbstractConvert createDoubleFieldConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return null;
    }

    @Override
    public AbstractConvert createEnumFieldConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return null;
    }

    @Override
    public AbstractConvert createFloatFieldConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return null;
    }

    @Override
    public AbstractConvert createIntegerFieldConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return null;
    }

    @Override
    public AbstractConvert createLongFieldConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return null;
    }

    @Override
    public AbstractConvert createMessageFieldConvert(Descriptors.FieldDescriptor fieldDescriptor, MessageConvert convert) {
        return null;
    }

    @Override
    public MessageConvert createMessageConvert(Descriptors.FieldDescriptor fieldDescriptor, List<AbstractConvert> fields) {
        return null;
    }

    @Override
    public AbstractConvert createRepeatedFieldConvert(Descriptors.FieldDescriptor fieldDescriptor, AbstractConvert field) {
        return null;
    }

    @Override
    public AbstractConvert createStringEmptryIsNullFieldConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return null;
    }

    @Override
    public AbstractConvert createStringFieldConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return null;
    }

    @Override
    public AbstractConvert createWktTimestampFieldConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return null;
    }

    @Override
    public AbstractConvert createWktWrapperFieldConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return null;
    }
}
