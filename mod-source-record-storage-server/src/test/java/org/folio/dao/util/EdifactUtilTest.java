package org.folio.dao.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.folio.rest.jaxrs.model.SourceRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import io.xlate.edi.stream.EDIStreamException;

@RunWith(BlockJUnit4ClassRunner.class)
public class EdifactUtilTest {

  private static final String SOURCE_RECORD_PATH = "src/test/resources/mock/sourceRecords/e4cfe577-4015-46d8-a54d-7c9b34796955.json";

  private SourceRecord sourceRecord;

  @Before
  public void readSourceRecord() throws JsonParseException, JsonMappingException, IOException {
    File file = new File(SOURCE_RECORD_PATH);
    sourceRecord = new ObjectMapper().readValue(file, SourceRecord.class);
  }

  @Test
  public void shouldFormatEdifact() throws IOException, EDIStreamException {
    String rawEdifact = sourceRecord.getRawRecord().getContent();
    String formattedEdifact = EdifactUtil.formatEdifact(rawEdifact);
    assertNotNull(formattedEdifact);
    assertEquals(sourceRecord.getParsedRecord().getFormattedContent(), formattedEdifact);
  }

  @Test
  public void shouldFormatEdifactAlreadyFormatted() throws IOException, EDIStreamException {
    String rawEdifact = sourceRecord.getParsedRecord().getFormattedContent();
    String formattedEdifact = EdifactUtil.formatEdifact(rawEdifact);
    assertNotNull(formattedEdifact);
    assertEquals(sourceRecord.getParsedRecord().getFormattedContent(), formattedEdifact);
  }

}
