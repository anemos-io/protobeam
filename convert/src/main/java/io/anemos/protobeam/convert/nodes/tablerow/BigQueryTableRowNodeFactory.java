package io.anemos.protobeam.convert.nodes.tablerow;

import com.google.protobuf.Descriptors;
import io.anemos.protobeam.convert.ConvertNodeFactory;
import io.anemos.protobeam.convert.nodes.AbstractConvert;
import io.anemos.protobeam.convert.nodes.AbstractMessageConvert;

import java.util.List;

public class BigQueryTableRowNodeFactory implements ConvertNodeFactory {


    @Override
    public AbstractConvert createBooleanFieldConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return new BooleanFieldConvert(fieldDescriptor);
    }

    @Override
    public AbstractConvert createBytesFieldConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return new BytesFieldConvert(fieldDescriptor);
    }

    @Override
    public AbstractConvert createDoubleFieldConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return new DoubleFieldConvert(fieldDescriptor);
    }

    @Override
    public AbstractConvert createEnumFieldConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return new EnumConvert(fieldDescriptor);
    }

    @Override
    public AbstractConvert createFlattenFieldConvert(Descriptors.FieldDescriptor fieldDescriptor, AbstractMessageConvert convert) {
        return new FlattenFieldConvert(fieldDescriptor, convert);
    }

    @Override
    public AbstractConvert createFloatFieldConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return new FloatFieldConvert(fieldDescriptor);
    }

    @Override
    public AbstractConvert createIntegerFieldConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return new IntegerFieldConvert(fieldDescriptor);
    }

    @Override
    public AbstractConvert createLongFieldConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return new LongFieldConvert(fieldDescriptor);
    }

    @Override
    public AbstractMessageConvert createMessageConvert(Descriptors.FieldDescriptor fieldDescriptor, List<AbstractConvert> fields) {
        return new MessageConvert(fieldDescriptor, fields);
    }

    @Override
    public AbstractConvert createMessageFieldConvert(Descriptors.FieldDescriptor fieldDescriptor, AbstractMessageConvert convert) {
        return new MessageFieldConvert(fieldDescriptor, convert);
    }

    @Override
    public AbstractConvert createNullableFieldConvert(Descriptors.FieldDescriptor fieldDescriptor, AbstractConvert field) {
        return new NullableFieldConvert(fieldDescriptor, field);
    }

    @Override
    public AbstractMessageConvert createPartitionColumnConvert(Descriptors.Descriptor messageDescriptor, AbstractMessageConvert convert) {
        return new PartitionColumnConvert(messageDescriptor, convert);
    }

    @Override
    public AbstractConvert createRenameFieldConvert(String renameTo, Descriptors.FieldDescriptor fieldDescriptor, AbstractConvert convert) {
        return new RenameFieldConvert(renameTo, fieldDescriptor, convert);
    }

    @Override
    public AbstractConvert createRepeatedFieldConvert(Descriptors.FieldDescriptor fieldDescriptor, AbstractConvert field) {
        return new RepeatedConvert(fieldDescriptor, field);
    }

    @Override
    public AbstractConvert createStringFieldConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return new StringFieldConvert(fieldDescriptor);
    }

    @Override
    public AbstractConvert createWktTimestampFieldConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return new WktTimestampFieldConvert(fieldDescriptor);
    }

    @Override
    public AbstractConvert createWktWrapperFieldConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return new WktWrapperFieldConvert(fieldDescriptor);
    }
}
