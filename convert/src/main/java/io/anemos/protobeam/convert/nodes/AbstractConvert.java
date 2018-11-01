package io.anemos.protobeam.convert.nodes;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

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

//    private void writeObject(ObjectOutputStream oos)
//            throws IOException {
//        oos.defaultWriteObject();
//        descriptor.toProto().writeTo(oos);
//    }
//
//    private void readObject(ObjectInputStream ois)
//            throws ClassNotFoundException, IOException {
//        ois.defaultReadObject();
//        Descriptors.FieldDescriptor
//
//   }}

//    private void writeObject(ObjectOutputStream oos)
//            throws IOException {
//        oos.defaultWriteObject();
//        oos.writeObject(address.getHouseNumber());
//    }
//
//    private void readObject(ObjectInputStream ois)
//            throws ClassNotFoundException, IOException {
//        ois.defaultReadObject();
//        Integer houseNumber = (Integer) ois.readObject();
//        Address a = new Address();
//        a.setHouseNumber(houseNumber);
//        this.setAddress(a);
//    }
}