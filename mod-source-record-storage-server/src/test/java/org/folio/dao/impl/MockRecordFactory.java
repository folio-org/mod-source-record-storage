package org.folio.dao.impl;

import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;

public class MockRecordFactory {

  public static Record getMockRecord(Snapshot snapshot) {
    String id = "39c68762-bc37-45e0-b523-58b4ed63f32f";
    return new Record().withId(id)
      .withMatchedId(id)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withMatchedProfileId("f9926e86-883b-4455-a807-fc5eeb9a951a")
      .withRecordType(Record.RecordType.MARC)
      .withOrder(0)
      .withGeneration(1)
      .withState(Record.State.ACTUAL);
  }

  public static Record[] getMockRecords(Snapshot snapshot1, Snapshot snapshot2) {
    String id1 = "233e18c0-9fab-4f5e-aa35-376af1bdc63e";
    String id2 = "79a4c560-802d-4031-a7a0-ff338fe734f3";
    String id3 = "e028c34e-3f11-41a0-a481-09d0b7af4a17";
    String id4 = "5a9f334b-9612-471a-8730-ccf42995f682";
    String id5 = "09454964-b0a6-409c-a26e-07e0399f0e72";
    return new Record[] {
      new Record()
        .withId(id1)
        .withMatchedId(id1)
        .withSnapshotId(snapshot1.getJobExecutionId())
        .withMatchedProfileId("f9926e86-883b-4455-a807-fc5eeb9a951a")
        .withRecordType(Record.RecordType.MARC)
        .withOrder(0)
        .withGeneration(1)
        .withState(Record.State.ACTUAL),
      new Record()
        .withId(id2)
        .withMatchedId(id2)
        .withSnapshotId(snapshot2.getJobExecutionId())
        .withMatchedProfileId("df7bf522-66e1-4b52-9d48-abd739f37934")
        .withRecordType(Record.RecordType.MARC)
        .withOrder(11)
        .withGeneration(1)
        .withState(Record.State.ACTUAL),
      new Record()
        .withId(id3)
        .withMatchedId(id3)
        .withSnapshotId(snapshot2.getJobExecutionId())
        .withMatchedProfileId("df7bf522-66e1-4b52-9d48-abd739f37934")
        .withRecordType(Record.RecordType.MARC)
        .withOrder(27)
        .withGeneration(2)
        .withState(Record.State.OLD),
      new Record()
        .withId(id4)
        .withMatchedId(id4)
        .withSnapshotId(snapshot1.getJobExecutionId())
        .withMatchedProfileId("f9926e86-883b-4455-a807-fc5eeb9a951a")
        .withRecordType(Record.RecordType.MARC)
        .withOrder(1)
        .withGeneration(1)
        .withState(Record.State.DRAFT),
      new Record()
        .withId(id5)
        .withMatchedId(id5)
        .withSnapshotId(snapshot2.getJobExecutionId())
        .withMatchedProfileId("df7bf522-66e1-4b52-9d48-abd739f37934")
        .withRecordType(Record.RecordType.MARC)
        .withOrder(101)
        .withGeneration(3)
        .withState(Record.State.ACTUAL)
    };
  }
  
}