package org.folio.dao.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.common.Json;
import org.apache.commons.io.FileUtils;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.marc4j.MarcException;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(BlockJUnit4ClassRunner.class)
public class MarcUtilTest {

  private static final String SOURCE_RECORD_PATH = "src/test/resources/mock/sourceRecords/d3cd3e1e-a18c-4f7c-b053-9aa50343394e.json";
  private static final String REORDERED_PARSED_RECORD = "src/test/resources/mock/sourceRecords/parsedRecords/reorderedParsedRecord.json";
  private static final String PARSED_RECORD = "src/test/resources/mock/sourceRecords/parsedRecords/parsedRecord.json";
  private static final String RAW_RECORD_PATH = "src/test/resources/mock/rawRecords/d3cd3e1e-a18c-4f7c-b053-9aa50343394e.json";
  private static final String REORDERING_RESULT_RECORD = "src/test/resources/mock/sourceRecords/parsedRecords/reorderingResultRecord.json";

  private SourceRecord sourceRecord;
  private SourceRecord reorderedSourceRecord;

  @Before
  public void readSourceRecord() throws IOException {
    File file = new File(SOURCE_RECORD_PATH);
    sourceRecord = new ObjectMapper().readValue(file, SourceRecord.class);
  }

  @Test
  public void shouldConvertRawMarcToMarcJson() throws IOException, MarcException {
    String rawMarc = new ObjectMapper().readValue(new File(RAW_RECORD_PATH), RawRecord.class).getContent();
    String marcJson = MarcUtil.rawMarcToMarcJson(rawMarc);
    assertNotNull(marcJson);
    assertEquals(rawMarc, MarcUtil.marcJsonToRawMarc(marcJson));
  }

  @Test
  public void shouldConvertRawMarcToTxtMarc() throws IOException, MarcException {
    String marcJson = new ObjectMapper().writeValueAsString(sourceRecord.getParsedRecord().getContent());
    String rawMarc = MarcUtil.marcJsonToRawMarc(marcJson);
    assertNotNull(rawMarc);
    String txtMarc = MarcUtil.rawMarcToTxtMarc(rawMarc);
    assertNotNull(txtMarc);
    assertEquals(sourceRecord.getParsedRecord().getFormattedContent().trim(), txtMarc.trim());
  }

  @Test
  public void shouldConvertMarcJsonToRawMarc() throws IOException, MarcException {
    String marcJson = new ObjectMapper().writeValueAsString(sourceRecord.getParsedRecord().getContent());
    String rawMarc = MarcUtil.marcJsonToRawMarc(marcJson);
    assertNotNull(rawMarc);
    assertEquals(Json.node(marcJson), Json.node(MarcUtil.rawMarcToMarcJson(rawMarc)));
  }

  @Test
  public void shouldConvertMarcJsonToTxtMarc() throws IOException, MarcException {
    String marcJson = new ObjectMapper().writeValueAsString(sourceRecord.getParsedRecord().getContent());
    String txtMarc = MarcUtil.marcJsonToTxtMarc(marcJson);
    assertNotNull(txtMarc);
    assertEquals(sourceRecord.getParsedRecord().getFormattedContent().trim(), txtMarc.trim());
  }

  @Test
  public void shouldReorderMarcRecordFields() throws IOException, MarcException {
    var systemReorderedRecordContent = readFileFromPath(PARSED_RECORD);
    var userOrderRecordContent = readFileFromPath(REORDERED_PARSED_RECORD);
    var expectedOrderRecord = readFileFromPath(REORDERING_RESULT_RECORD);

    var actualOrderRecord = MarcUtil.reorderMarcRecordFields(userOrderRecordContent, systemReorderedRecordContent);

    assertNotNull(actualOrderRecord);
    assertEquals(formatContent(expectedOrderRecord), formatContent(actualOrderRecord));
  }

  private static String readFileFromPath(String path) throws IOException {
    return new String(FileUtils.readFileToByteArray(new File(path)));
  }

  private String formatContent(String content) {
    return content.replaceAll("\\s", "");
  }
}
