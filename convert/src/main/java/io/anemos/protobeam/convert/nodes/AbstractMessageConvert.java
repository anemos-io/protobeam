package io.anemos.protobeam.convert.nodes;

import com.google.protobuf.Descriptors;

public abstract class AbstractMessageConvert<T, TARGET, SOURCE> extends AbstractConvert<T, TARGET, SOURCE> {

    public AbstractMessageConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

}
