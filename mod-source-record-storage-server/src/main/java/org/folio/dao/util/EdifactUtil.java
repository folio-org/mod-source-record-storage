package org.folio.dao.util;

import static io.xlate.edi.stream.EDIStreamConstants.Delimiters.SEGMENT;
import static java.lang.String.format;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import io.xlate.edi.stream.EDIInputFactory;
import io.xlate.edi.stream.EDIStreamException;
import io.xlate.edi.stream.EDIStreamReader;

/**
 * Utility class for formatting EDIFACT records
 */
public class EdifactUtil {

  private final static String IGNORED_CODE = "ZZ";

  private EdifactUtil() { }

  public static String formatEdifact(String edifact) throws IOException, EDIStreamException {
    if (edifact.contains("\n")) {
      // already formattted
      return edifact;
    }
    String segmentDelimiter = inferSegmentDelimiter(edifact);
    return edifact.replaceAll(segmentDelimiter, format("%s\n", segmentDelimiter));
  }

  private static String inferSegmentDelimiter(String edifact) throws IOException, EDIStreamException {
    Map<String, Character> delimiters = new HashMap<>();
    try (
      InputStream stream = new ByteArrayInputStream(edifact.getBytes());
      EDIStreamReader reader = EDIInputFactory.newFactory().createEDIStreamReader(stream);
    ) {
      while (reader.hasNext()) {
        switch (reader.next()) {
          case START_INTERCHANGE:
            delimiters = reader.getDelimiters();
            break;
          case ELEMENT_DATA_ERROR:
          case ELEMENT_OCCURRENCE_ERROR:
          case SEGMENT_ERROR:
            if (!reader.getText().equals(IGNORED_CODE)) {
              throw new EDIStreamException(format("%s: %s", reader.getErrorType(), reader.getText()));
            }
            break;
          default:
            break;
        }
      }
    }
    return String.valueOf(delimiters.get(SEGMENT));
  }

}
