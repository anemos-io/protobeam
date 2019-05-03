package io.anemos.protobeam.convert.nodes.tablerow;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractConvert;
import io.anemos.protobeam.convert.nodes.AbstractMessageConvert;
import java.util.List;
import java.util.Map;

class MessageConvert extends AbstractMessageConvert<Object, TableRow, Map<String, Object>> {

  private List<AbstractConvert> fields;

  public MessageConvert(Descriptors.FieldDescriptor descriptor, List<AbstractConvert> fields) {
    super(descriptor);

    this.fields = fields;
  }

  @Override
  public Object fromProtoValue(Object in) {
    return in;
  }

  @Override
  public void fromProto(Message message, TableRow row) {
    fields.forEach(e -> e.fromProto(message, row));
  }

  @Override
  public void toProto(Map row, Message.Builder builder) {
    fields.forEach(e -> e.toProto(row, builder));
  }
}
