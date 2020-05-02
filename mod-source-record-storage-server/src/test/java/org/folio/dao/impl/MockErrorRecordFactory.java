package org.folio.dao.impl;

import static org.folio.dao.impl.MockParsedRecordFactory.parsedMarc;
import static org.folio.dao.impl.MockSourceRecordFactory.getParsedRecordContent;

import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.Record;

public class MockErrorRecordFactory {

  public static ErrorRecord getMockErrorRecord(Record mockRecord) {
    return new ErrorRecord()
      .withId(mockRecord.getId())
      .withContent(parsedMarc)
      .withDescription("Oops... something happened");
  }

  public static ErrorRecord[] getMockErrorRecords(Record[] mockRecords) {
    return new ErrorRecord[] {
      new ErrorRecord()
        .withId(mockRecords[0].getId())
        .withContent(getParsedRecordContent(0))
        .withDescription("Oops... something happened"),
      new ErrorRecord()
        .withId(mockRecords[1].getId())
        .withContent(getParsedRecordContent(1))
        .withDescription("Something went wrong"),
      new ErrorRecord()
        .withId(mockRecords[2].getId())
        .withContent(getParsedRecordContent(2))
        .withDescription("Oops... something happened"),
      new ErrorRecord()
        .withId(mockRecords[3].getId())
        .withContent(getParsedRecordContent(3))
        .withDescription("Oops... something happened"),
      new ErrorRecord()
        .withId(mockRecords[4].getId())
        .withContent(getParsedRecordContent(4))
        .withDescription("Failed parsing MARC record")
    };
  }

}