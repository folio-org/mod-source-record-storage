package org.folio.dao.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.xlate.edi.stream.EDIStreamException;
import java.io.File;
import java.io.IOException;
import org.folio.TestUtil;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EdifactUtilTest {

  private static final String SOURCE_RECORD_PATH = "src/test/resources/mock/sourceRecords/e4cfe577-4015-46d8-a54d-7c9b34796955.json";
  private static final String SOURCE_RECORD_WITH_IGNORED_CODES_PATH = "src/test/resources/mock/sourceRecords/4ca9d8ac-9de5-432a-83ee-15832f09e868.json";
  private static final String RAW_EDIFACT_RECORD_CONTENT_SAMPLE_PATH = "src/test/resources/rawEdifactRecordContent.sample";

  private static final String INVALID_RAW_EDIFACT_RECORD =
    "UNA:+.? '" +
    "UNB+UNOC:2+1694510:1943+10" +
    "UNH+30+INVOIC:D:96A:UN:EAN008'" +
    "BGM+380+475463'" +
    "DTM+137:20130919:102'" +
    "CUX+2:USD:4'" +
    "ALC+C++++TX::28:TAX'" +
    "MOA+8:69.09'" +
    "LIN+1++9780199588459:EN'" +
    "IMD+L+050+:::BLACK MARKET BRITAIN, 1939-1955.'" +
    "IMD+L+010+:::ROODHOUSE, MARK'" +
    "IMD+L+110+:::OXFORD'" +
    "QTY+47:1'" +
    "MOA+203:103.13'" +
    "PRI+AAB:125'" +
    "PRI+AAA:103.13'" +
    "RFF+LI:POL-17653'" +
    "RFF+SLI:99954761992'" +
    "UNS+S'" +
    "CNT+1:19'" +
    "CNT+2:19'" +
    "MOA+9:1818.28'" +
    "MOA+79:1818.28'" +
    "UNT+217+33'";

  private SourceRecord sourceRecord;

  @BeforeEach
  void readSourceRecord() throws IOException {
    File file = new File(SOURCE_RECORD_PATH);
    sourceRecord = new ObjectMapper().readValue(file, SourceRecord.class);
  }

  @Test
  void shouldFormatEdifact() throws IOException, EDIStreamException {
    String rawEdifact = new ObjectMapper().readValue(
      TestUtil.readFileFromPath(RAW_EDIFACT_RECORD_CONTENT_SAMPLE_PATH), String.class);
    String formattedEdifact = EdifactUtil.formatEdifact(rawEdifact);
    assertNotNull(formattedEdifact);
    assertEquals(sourceRecord.getParsedRecord().getFormattedContent(), formattedEdifact);
  }

  @Test
  void shouldFormatEdifactAlreadyFormatted() throws IOException, EDIStreamException {
    String rawEdifact = sourceRecord.getParsedRecord().getFormattedContent();
    String formattedEdifact = EdifactUtil.formatEdifact(rawEdifact);
    assertNotNull(formattedEdifact);
    assertEquals(sourceRecord.getParsedRecord().getFormattedContent(), formattedEdifact);
  }

  @Test
  void shouldFormatEdifactWithIgnoredCodeValues() throws IOException, EDIStreamException {
    SourceRecord sourceRecord = new ObjectMapper().readValue(new File(SOURCE_RECORD_WITH_IGNORED_CODES_PATH), SourceRecord.class);
    String rawEdifact = sourceRecord.getParsedRecord().getFormattedContent();
    String formattedEdifact = EdifactUtil.formatEdifact(rawEdifact);
    assertNotNull(formattedEdifact);
    assertEquals(sourceRecord.getParsedRecord().getFormattedContent(), formattedEdifact);
  }

  @Test
  void shouldThrowExceptionAttemptingFormatEdifact() {
    assertThrows(EDIStreamException.class, () -> EdifactUtil.formatEdifact(INVALID_RAW_EDIFACT_RECORD));
  }

}
