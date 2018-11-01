package io.anemos.protobeam.convert;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractConvert;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ProtoBigQueryExecutionPlan<T extends Message> implements Serializable {

    private Class<T> messageClass;
    private transient AbstractConvert convert;
    private transient Descriptors.Descriptor descriptor;

    public ProtoBigQueryExecutionPlan(Message message) {
        this((Class<T>) message.getClass());
    }

    public ProtoBigQueryExecutionPlan(Class<T> messageClass) {
        this.messageClass = messageClass;
        inferDescriptor();
        this.convert = new ProtoBigQueryPlanner(this.descriptor).createPlan();
    }

    private void inferDescriptor() {
        try {
            //public static final com.google.protobuf.Descriptors.Descriptor
            //getDescriptor() {
            Method method = messageClass.getMethod("getDescriptor");
            descriptor = (Descriptors.Descriptor) method.invoke(null);
        } catch (IllegalAccessException e) {
            e.printStackTrace();

        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    public TableRow convert(T message) {
        TableRow row = new TableRow();
        convert.convert(message, row);
        return row;
    }

    public T convertToProto(TableRow row) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        convert.convertToProto(builder, row);
        return (T) builder.build();
    }

//    public static ProtoBigQueryExecutionPlan create(Descriptors.Descriptor descriptor) {
//        return new ProtoBigQueryExecutionPlan(descriptor);
//    }


    private void writeObject(ObjectOutputStream oos)
            throws IOException {
        oos.defaultWriteObject();
        if (messageClass.equals(DynamicMessage.class)) {
            DescriptorSerializer.writeObject(oos, descriptor);
        }
    }

    private void readObject(ObjectInputStream ois)
            throws ClassNotFoundException, IOException {
        ois.defaultReadObject();
        if (messageClass.equals(DynamicMessage.class)) {
            descriptor = DescriptorSerializer.readObject(ois);
        } else {
            inferDescriptor();
        }
        convert = new ProtoBigQueryPlanner(descriptor).createPlan();
    }

}
