package io.anemos.protobeam.convert;


import com.google.protobuf.Descriptors;
import io.anemos.protobeam.examples.ProtoBeamOptionMessage;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SchemaProtoToBigQueryTest extends AbstractProtoBigQueryTest {


    @Test
    public void testSchemaForSchemaTest() {
        ProtoBeamOptionMessage x = ProtoBeamOptionMessage.newBuilder().build();
        Descriptors.Descriptor descriptor = x.getDescriptorForType();

        String modelRef = "{fields=[{name=test_name, type=STRING}, {name=test_index, type=INT64}, {description=@deprecated\n" +
                ", name=option_deprecated, type=STRING}, {description=A very detailed description, name=option_description, type=STRING}]}";
        SchemaProtoToBigQueryModel model = new SchemaProtoToBigQueryModel();
        assertEquals(modelRef, model.getSchema(descriptor).toString());

<<<<<<< Updated upstream
        String apiRef = "Schema{fields=[Field{name=test_name, value=Type{value=STRING, fields=null}, mode=null, description=null}, Field{name=test_index, value=Type{value=INTEGER, fields=null}, mode=null, description=null}, Field{name=option_deprecated, value=Type{value=STRING, fields=null}, mode=null, description=@deprecated\n" +
                "}, Field{name=option_description, value=Type{value=STRING, fields=null}, mode=null, description=A very detailed description}]}";
=======
        String apiRef = "Schema{fields=[" +
                "Field{name=test_name, type=STRING, mode=REQUIRED, description=null}, " +
                "Field{name=test_index, type=INTEGER, mode=REQUIRED, description=null}, " +
                "Field{name=option_deprecated, type=STRING, mode=REQUIRED, description=@deprecated\n" +"}, " + //TODO check newline at deprication
                "Field{name=option_description, type=STRING, mode=REQUIRED, description=A very detailed description}]}";
>>>>>>> Stashed changes
        SchemaProtoToBigQueryApi api = new SchemaProtoToBigQueryApi();
        assertEquals(apiRef, api.getSchema(descriptor).toString());
    }

}
