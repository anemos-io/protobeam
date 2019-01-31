package io.anemos.protobeam.convert;

import com.google.protobuf.Descriptors;
import io.anemos.protobeam.convert.nodes.AbstractConvert;
import io.anemos.protobeam.convert.nodes.MessageConvert;

import java.util.List;

public interface ConvertNodeFactory {

    AbstractConvert createBooleanFieldConvert(Descriptors.FieldDescriptor fieldDescriptor);

    AbstractConvert createBytesFieldConvert(Descriptors.FieldDescriptor fieldDescriptor);

    AbstractConvert createDoubleFieldConvert(Descriptors.FieldDescriptor fieldDescriptor);

    AbstractConvert createEnumFieldConvert(Descriptors.FieldDescriptor fieldDescriptor);

    AbstractConvert createFloatFieldConvert(Descriptors.FieldDescriptor fieldDescriptor);

    AbstractConvert createIntegerFieldConvert(Descriptors.FieldDescriptor fieldDescriptor);

    AbstractConvert createLongFieldConvert(Descriptors.FieldDescriptor fieldDescriptor);

    AbstractConvert createMessageFieldConvert(Descriptors.FieldDescriptor fieldDescriptor, MessageConvert convert);

    MessageConvert createMessageConvert(Descriptors.FieldDescriptor fieldDescriptor, List<AbstractConvert> fields);

    AbstractConvert createRepeatedFieldConvert(Descriptors.FieldDescriptor fieldDescriptor, AbstractConvert field);

    @Deprecated
    AbstractConvert createStringEmptryIsNullFieldConvert(Descriptors.FieldDescriptor fieldDescriptor);

    AbstractConvert createStringFieldConvert(Descriptors.FieldDescriptor fieldDescriptor);

    AbstractConvert createWktTimestampFieldConvert(Descriptors.FieldDescriptor fieldDescriptor);

    AbstractConvert createWktWrapperFieldConvert(Descriptors.FieldDescriptor fieldDescriptor);

}
