package io.anemos.protobeam.convert;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Int32Value;
import com.google.protobuf.InvalidProtocolBufferException;
import io.anemos.protobeam.examples.ProtoBeamBasicNullablePrimitive;
import io.anemos.protobeam.examples.ProtoBeamBasicPrimitive;
import io.anemos.protobeam.examples.ProtoBeamBasicSpecial;
import org.junit.Test;

import static org.junit.Assert.*;

public class AssumptionTest {

    @Test
    public void testPrimitive() throws InvalidProtocolBufferException {
        ProtoBeamBasicPrimitive empty = ProtoBeamBasicPrimitive.newBuilder()
                .build();
        assertEquals(0, empty.getPrimitiveInt32());
        assertFalse(empty.hasField(ProtoBeamBasicPrimitive.getDescriptor().getFields().get(5)));
        empty = ProtoBeamBasicPrimitive.parseFrom(empty.toByteString());
        assertEquals(0, empty.getPrimitiveInt32());
        assertFalse(empty.hasField(ProtoBeamBasicPrimitive.getDescriptor().getFields().get(5)));
    }

    /**
     * Assume we can only detect that a Wrapper is null is by looking at the
     * empty.has... method. This is valid for all Messages.
     */
    @Test
    public void testWrapper() throws InvalidProtocolBufferException {
        ProtoBeamBasicNullablePrimitive empty = ProtoBeamBasicNullablePrimitive.newBuilder()
                .build();
        assertFalse(empty.hasNullableInt32());
        assertEquals(0, empty.getNullableInt32().getValue());
        empty = ProtoBeamBasicNullablePrimitive.parseFrom(empty.toByteString());
        assertFalse(empty.hasNullableInt32());
        assertEquals(0, empty.getNullableInt32().getValue());
        ProtoBeamBasicNullablePrimitive primitive = ProtoBeamBasicNullablePrimitive.newBuilder()
                .setNullableInt32(Int32Value.newBuilder()
                        .build())
                .build();
        assertTrue(primitive.hasNullableInt32());
        assertEquals(0, primitive.getNullableInt32().getValue());
        primitive = ProtoBeamBasicNullablePrimitive.parseFrom(primitive.toByteString());
        assertTrue(primitive.hasNullableInt32());
        assertEquals(0, primitive.getNullableInt32().getValue());
        ProtoBeamBasicNullablePrimitive pv = ProtoBeamBasicNullablePrimitive.newBuilder()
                .setNullableInt32(Int32Value.newBuilder()
                        .setValue(0)
                        .build())
                .build();
        assertTrue(pv.hasNullableInt32());
        assertEquals(0, pv.getNullableInt32().getValue());
        pv = ProtoBeamBasicNullablePrimitive.parseFrom(pv.toByteString());
        assertTrue(pv.hasNullableInt32());
        assertEquals(0, pv.getNullableInt32().getValue());
    }

    @Test
    public void testOneOf() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor intDsc = ProtoBeamBasicSpecial.getDescriptor().findFieldByNumber(ProtoBeamBasicSpecial.ONEOF_INT32_FIELD_NUMBER);
        Descriptors.FieldDescriptor strDsc = ProtoBeamBasicSpecial.getDescriptor().findFieldByNumber(ProtoBeamBasicSpecial.ONEOF_STRING_FIELD_NUMBER);
        Descriptors.OneofDescriptor oofDsc = ProtoBeamBasicSpecial.getDescriptor().getOneofs().get(0);
        assertEquals("special_oneof", oofDsc.getName());

        ProtoBeamBasicSpecial m0 = ProtoBeamBasicSpecial.newBuilder()
                .build();
        assertEquals(0, m0.getSpecialOneofCase().getNumber());
        assertFalse(m0.hasOneof(oofDsc));
        assertFalse(m0.hasField(intDsc));
        assertFalse(m0.hasField(strDsc));
        m0 = ProtoBeamBasicSpecial.parseFrom(m0.toByteString());
        assertEquals(0, m0.getSpecialOneofCase().getNumber());
        assertFalse(m0.hasOneof(oofDsc));
        assertFalse(m0.hasField(intDsc));
        assertFalse(m0.hasField(strDsc));

        ProtoBeamBasicSpecial m1 = ProtoBeamBasicSpecial.newBuilder()
                .setOneofInt32(0)
                .build();
        assertEquals(ProtoBeamBasicSpecial.ONEOF_INT32_FIELD_NUMBER, m1.getSpecialOneofCase().getNumber());
        assertTrue(m1.hasOneof(oofDsc));
        assertTrue(m1.hasField(intDsc));
        assertFalse(m1.hasField(strDsc));
        m1 = ProtoBeamBasicSpecial.parseFrom(m1.toByteString());
        assertEquals(ProtoBeamBasicSpecial.ONEOF_INT32_FIELD_NUMBER, m1.getSpecialOneofCase().getNumber());
        assertTrue(m1.hasOneof(oofDsc));
        assertTrue(m1.hasField(intDsc));
        assertFalse(m1.hasField(strDsc));

    }

}
