package io.anemos.protobeam.convert.nodes;

import com.google.protobuf.Descriptors;

public abstract class AbstractMessageConvert<T, IN, OUT> extends AbstractConvert<T, IN, OUT> {

    public AbstractMessageConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

}
