package io.anemos.protobeam.convert;

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

public class AbstractExecutionPlan<T extends Message> implements Serializable {

    protected Class<T> messageClass;
    protected transient AbstractConvert convert;
    protected transient Descriptors.Descriptor descriptor;
    protected String factoryClassName;

    public AbstractExecutionPlan(Class<T> messageClass, ConvertNodeFactory factory) {
        this.messageClass = messageClass;
        inferDescriptor();
        this.convert = new ProtoConvertPlanner(this.descriptor, factory).createPlan();
        this.factoryClassName = factory.getClass().getName();
    }

    protected void inferDescriptor() {
        try {
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

    protected T convertToConcrete(DynamicMessage.Builder builder) {
        if (!this.messageClass.equals(DynamicMessage.class)) {
            try {
                Method method = messageClass.getMethod("parseFrom", byte[].class);
                return (T) method.invoke(null, builder.build().toByteArray());
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            }
        }
        return (T) builder.build();
    }

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
        try {
            convert = new ProtoConvertPlanner(descriptor, (ConvertNodeFactory) Class.forName(factoryClassName).newInstance()).createPlan();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

}
