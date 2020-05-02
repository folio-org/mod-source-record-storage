package org.folio.dao.impl;

import static org.folio.dao.impl.MockSourceRecordFactory.getParsedRecordContent;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.folio.rest.impl.TestUtil;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;

import io.vertx.core.json.JsonObject;

public class MockParsedRecordFactory {

  private static final String PARSED_RECORD_CONTENT_SAMPLE_PATH = "src/test/resources/parsedRecordContent.sample";

  public static String parsedMarc;

  static {
    try {
      parsedMarc = new ObjectMapper().readValue(TestUtil.readFileFromPath(PARSED_RECORD_CONTENT_SAMPLE_PATH), JsonObject.class).encode();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static ParsedRecord getMockParsedRecord(Record mockRecord) {
    return new ParsedRecord()
      .withId(mockRecord.getId())
      .withContent(parsedMarc);
  }

  public static ParsedRecord[] getMockParsedRecords(Record[] mockRecords) {
    return new ParsedRecord[] {
      new ParsedRecord()
        .withId(mockRecords[0].getId())
        .withContent(getParsedRecordContent(0)),
      new ParsedRecord()
        .withId(mockRecords[1].getId())
        .withContent(getParsedRecordContent(1)),
      new ParsedRecord()
        .withId(mockRecords[2].getId())
        .withContent(getParsedRecordContent(2)),
      new ParsedRecord()
        .withId(mockRecords[3].getId())
        .withContent(getParsedRecordContent(3)),
      new ParsedRecord()
        .withId(mockRecords[4].getId())
        .withContent(getParsedRecordContent(4)),
    };
  }

  

}