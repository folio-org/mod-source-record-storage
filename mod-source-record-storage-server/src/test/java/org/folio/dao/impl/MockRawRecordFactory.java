package org.folio.dao.impl;

import static org.folio.dao.impl.MockSourceRecordFactory.getRawRecordContent;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.folio.rest.impl.TestUtil;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;

public class MockRawRecordFactory {

  private static final String RAW_RECORD_CONTENT_SAMPLE_PATH = "src/test/resources/rawRecordContent.sample";
  
  public static String rawMarc;

  static {
    try {
      rawMarc = new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_RECORD_CONTENT_SAMPLE_PATH), String.class);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static RawRecord getMockRawRecord(Record mockRecord) {
    return new RawRecord()
      .withId(mockRecord.getId())
      .withContent(rawMarc);
  }

  public static RawRecord[] getMockRawRecords(Record[] mockRecords) {
    return new RawRecord[] {
      new RawRecord()
        .withId(mockRecords[0].getId())
        .withContent(getRawRecordContent(0)),
      new RawRecord()
        .withId(mockRecords[1].getId())
        .withContent(getRawRecordContent(1)),
      new RawRecord()
        .withId(mockRecords[2].getId())
        .withContent(getRawRecordContent(2)),
      new RawRecord()
        .withId(mockRecords[3].getId())
        .withContent(getRawRecordContent(3)),
      new RawRecord()
        .withId(mockRecords[4].getId())
        .withContent(getRawRecordContent(4))
    };
  }

}