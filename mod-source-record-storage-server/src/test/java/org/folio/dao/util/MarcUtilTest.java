package org.folio.dao.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.github.tomakehurst.wiremock.common.Json;

import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.tools.utils.ObjectMapperTool;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.marc4j.MarcException;

import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class MarcUtilTest {

  private static final String SOURCE_RECORD_PATH = "src/test/resources/mock/sourceRecords/d3cd3e1e-a18c-4f7c-b053-9aa50343394e.json";

  private SourceRecord sourceRecord;

  @Before
  public void readSourceRecord() throws JsonParseException, JsonMappingException, IOException {
    File file = new File(SOURCE_RECORD_PATH);
    sourceRecord = ObjectMapperTool.getDefaultMapper().readValue(file, SourceRecord.class);
  }

  @Test
  public void shouldConvertRawMarcToMarcJson() throws IOException, MarcException {
    String rawMarc = sourceRecord.getRawRecord().getContent();
    String marcJson = MarcUtil.rawMarcToMarcJson(rawMarc);
    assertNotNull(marcJson);
    assertEquals(rawMarc, MarcUtil.marcJsonToRawMarc(marcJson));
  }

  @Test
  public void shouldConvertRawMarcToTxtMarc() throws IOException, MarcException {
    String marcJson = ObjectMapperTool.getDefaultMapper().writeValueAsString(sourceRecord.getParsedRecord().getContent());
    String rawMarc = MarcUtil.marcJsonToRawMarc(marcJson);
    assertNotNull(rawMarc);
    String txtMarc = MarcUtil.rawMarcToTxtMarc(rawMarc);
    assertNotNull(txtMarc);
    assertEquals(sourceRecord.getParsedRecord().getFormattedContent().trim(), txtMarc.trim());
  }

  @Test
  public void shouldConvertMarcJsonToRawMarc() throws IOException, MarcException {
    String marcJson = ObjectMapperTool.getDefaultMapper().writeValueAsString(sourceRecord.getParsedRecord().getContent());
    String rawMarc = MarcUtil.marcJsonToRawMarc(marcJson);
    assertNotNull(rawMarc);
    assertEquals(Json.node(marcJson), Json.node(MarcUtil.rawMarcToMarcJson(rawMarc)));
  }

  @Test
  public void shouldConvertMarcJsonToTxtMarc() throws IOException, MarcException {
    String marcJson = ObjectMapperTool.getDefaultMapper().writeValueAsString(sourceRecord.getParsedRecord().getContent());
    String txtMarc = MarcUtil.marcJsonToTxtMarc(marcJson);
    assertNotNull(txtMarc);
    assertEquals(sourceRecord.getParsedRecord().getFormattedContent().trim(), txtMarc.trim());
  }

}