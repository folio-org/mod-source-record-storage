package org.folio.dao.impl;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.folio.rest.impl.TestUtil;
import org.folio.rest.jaxrs.model.SourceRecord;

import io.vertx.core.json.JsonObject;

public class MockSourceRecordFactory {

  private static final SourceRecord[] SOURCE_RECORDS = new SourceRecord[5];

  static {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      SOURCE_RECORDS[0] = objectMapper.readValue(TestUtil.readFileFromPath("src/test/resources/mock/sourceRecord1.json"), SourceRecord.class);
      SOURCE_RECORDS[1] = objectMapper.readValue(TestUtil.readFileFromPath("src/test/resources/mock/sourceRecord2.json"), SourceRecord.class);
      SOURCE_RECORDS[2] = objectMapper.readValue(TestUtil.readFileFromPath("src/test/resources/mock/sourceRecord3.json"), SourceRecord.class);
      SOURCE_RECORDS[3] = objectMapper.readValue(TestUtil.readFileFromPath("src/test/resources/mock/sourceRecord4.json"), SourceRecord.class);
      SOURCE_RECORDS[4] = objectMapper.readValue(TestUtil.readFileFromPath("src/test/resources/mock/sourceRecord5.json"), SourceRecord.class);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static String getRawRecordContent(int i) {
    return SOURCE_RECORDS[i].getRawRecord().getContent();
  }

  public static String getParsedRecordContent(int i) {
    return JsonObject.mapFrom(SOURCE_RECORDS[i].getParsedRecord().getContent()).encode();
  }

}