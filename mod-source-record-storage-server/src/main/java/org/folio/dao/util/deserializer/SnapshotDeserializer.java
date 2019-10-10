package org.folio.dao.util.deserializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.tools.utils.ObjectMapperTool;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Deserializes JSON content to {@link Snapshot} entity ignoring 'id' field in JSON
 */
@Component
public class SnapshotDeserializer extends JsonDeserializer<Snapshot> {

  @Override
  public Snapshot deserialize(JsonParser parser, DeserializationContext context) throws IOException {
    ObjectCodec objectCodec = parser.getCodec();
    JsonNode node = objectCodec.readTree(parser);
    ((ObjectNode) node).remove("id");
    return ObjectMapperTool.getDefaultMapper().treeToValue(node, Snapshot.class);
  }
}
