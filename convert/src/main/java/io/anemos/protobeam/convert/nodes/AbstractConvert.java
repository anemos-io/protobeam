package io.anemos.protobeam.convert.nodes;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

public abstract class AbstractConvert<FIELD, TARGET, SOURCE> {

    protected Descriptors.FieldDescriptor fieldDescriptor;

    public AbstractConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    /**
     * Convert an object that came from a proto field into the format that the target ccontainer
     * expect.
     *
     * @param in proto field object
     * @return container field object
     */
    public abstract Object fromProtoValue(Object in);

    /**
     * Convert a proto message into a target container.
     *
     * @param message
     * @param row
     */
    public abstract void fromProto(Message message, TARGET row);

    /**
     * Convert an object that came from the source container and convert it into the type that
     * Protobuf expects.
     *
     * @param in
     * @return
     */
    public FIELD toProtoValue(Object in) {
        return (FIELD) in;
    }

    /**
     * Convert a source container into a proto message.
     *
     * @param row
     * @param builder
     */
    public abstract void toProto(SOURCE row, Message.Builder builder);

}