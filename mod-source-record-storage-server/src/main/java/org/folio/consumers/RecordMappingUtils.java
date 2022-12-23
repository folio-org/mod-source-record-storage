package org.folio.consumers;

import io.vertx.core.json.JsonObject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.jaxrs.model.Record;
import org.marc4j.MarcJsonReader;
import org.marc4j.MarcJsonWriter;
import org.marc4j.MarcReader;
import org.marc4j.MarcStreamWriter;

//todo: replace after this methods become public in mod-data-import-processing-core.
// Maybe not possible because same dto is built in different packages (org.folio.rest.jaxrs.model.Record, org.folio.Record).
// In such case this should be replaces after resolving issue with having same dtos built by multiple modules
public final class RecordMappingUtils {

  private static final String ERROR_RECORD_PARSING_MSG = "Failed to parse record from payload";
  private static final Logger LOGGER = LogManager.getLogger();

  private RecordMappingUtils() {}

  public static org.marc4j.marc.Record readParsedContentToObjectRepresentation(Record srsRecord) {
    var existingRecordReader = buildMarcReader(srsRecord);
    if (existingRecordReader.hasNext()) {
      return existingRecordReader.next();
    } else {
      LOGGER.error(ERROR_RECORD_PARSING_MSG);
      throw new IllegalArgumentException(ERROR_RECORD_PARSING_MSG);
    }
  }

  public static String mapObjectRepresentationToParsedContentJsonString(org.marc4j.marc.Record marcRecord) {
    var os = new ByteArrayOutputStream();
    var streamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
    var jsonWriter = new MarcJsonWriter(os);
    streamWriter.write(marcRecord);
    jsonWriter.write(marcRecord);
    return os.toString().trim();
  }

  private static MarcReader buildMarcReader(Record srsRecord) {
    var parsedContent = srsRecord.getParsedRecord().getContent() instanceof String
      ? new JsonObject(srsRecord.getParsedRecord().getContent().toString())
      : JsonObject.mapFrom(srsRecord.getParsedRecord().getContent());

    return new MarcJsonReader(new ByteArrayInputStream(parsedContent
      .toString()
      .getBytes(StandardCharsets.UTF_8)));
  }
}
