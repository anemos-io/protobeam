package io.anemos.protobeam.convert;

import com.google.protobuf.Descriptors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

class FieldExplorer {

    private Map<Integer, Descriptors.OneofDescriptor> oneofmap;
    private List<Descriptors.FieldDescriptor> fields;
    private List<OrderedField> orderedFields;

    FieldExplorer(Descriptors.Descriptor descriptor) {
        oneofmap = new HashMap<>();
        for (Descriptors.OneofDescriptor oneof : descriptor.getOneofs()) {
            oneofmap.put(oneof.getField(0).getIndex(), oneof);
        }
        fields = descriptor.getFields();

        orderedFields = new ArrayList<>(fields.size());
        for (int ix = 0; ix < fields.size(); ) {
            Descriptors.FieldDescriptor fieldDescriptor = fields.get(ix);
            Descriptors.OneofDescriptor oneof = oneofmap.get(ix);
            if (oneof != null) {
                for (Descriptors.FieldDescriptor oneofField : oneof.getFields()) {
                    orderedFields.add(new OrderedField(
                            oneofField,
                            true
                    ));
                    ix++;
                }
            } else {
                orderedFields.add(new OrderedField(
                        fieldDescriptor,
                        false
                ));
                ix++;
            }
        }
    }

    public static FieldExplorer of(Descriptors.Descriptor descriptor) {
        return new FieldExplorer(descriptor);
    }

    public int size() {
        return fields.size();
    }

    public boolean isOneOf(int ix) {
        return orderedFields.get(ix).isOneOf;
    }

    public Descriptors.FieldDescriptor get(int ix) {
        return orderedFields.get(ix).fieldDescriptor;
    }

    public void forEach(Consumer<OrderedField> action) {
        orderedFields.forEach(action);
    }

    public class OrderedField {
        Descriptors.FieldDescriptor fieldDescriptor;
        boolean isOneOf;

        public OrderedField(Descriptors.FieldDescriptor fieldDescriptor, boolean isOneOf) {
            this.fieldDescriptor = fieldDescriptor;
            this.isOneOf = isOneOf;
        }
    }

}
