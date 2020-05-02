package org.folio.dao.impl;

import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;

public class MockRecordFactory {

  public static Record getMockRecord(Snapshot snapshot) {
    String id = "39c68762-bc37-45e0-b523-58b4ed63f32f";
    return new Record().withId(id)
      .withMatchedId(id)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withMatchedProfileId("f9926e86-883b-4455-a807-fc5eeb9a951a")
      .withOrder(0)
      .withGeneration(1)
      .withRecordType(Record.RecordType.MARC)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId("47b4fbce-6200-48ad-af71-44d5246154d7"));
  }

  public static Record[] getMockRecords(Snapshot snapshot1, Snapshot snapshot2) {
    String id1 = "009286d6-f89e-4881-9562-11158f02664a";
    String id2 = "e567b8e2-a45b-45f1-a85a-6b6312bdf4d8";
    String id3 = "d3cd3e1e-a18c-4f7c-b053-9aa50343394e";
    String id4 = "7293f287-bb51-41f5-805d-00ff18a1f791";
    String id5 = "13f0b043-4fdd-4ba9-b3a8-a35b2ac2a30a";
    return new Record[] {
      new Record()
        .withId(id1)
        .withMatchedId(id1)
        .withSnapshotId(snapshot1.getJobExecutionId())
        .withMatchedProfileId("f9926e86-883b-4455-a807-fc5eeb9a951a")
        .withOrder(0)
        .withGeneration(1)
        .withRecordType(Record.RecordType.MARC)
        .withState(Record.State.ACTUAL)
        .withExternalIdsHolder(new ExternalIdsHolder()
          .withInstanceId("6eee8eb9-db1a-46e2-a8ad-780f19974efa")),
      new Record()
        .withId(id2)
        .withMatchedId(id2)
        .withSnapshotId(snapshot2.getJobExecutionId())
        .withMatchedProfileId("df7bf522-66e1-4b52-9d48-abd739f37934")
        .withOrder(11)
        .withGeneration(1)
        .withRecordType(Record.RecordType.MARC)
        .withState(Record.State.ACTUAL)
        .withExternalIdsHolder(new ExternalIdsHolder()
          .withInstanceId("54cc0262-76df-4cac-acca-b10e9bc5c79a")),
      new Record()
        .withId(id3)
        .withMatchedId(id1)
        .withSnapshotId(snapshot2.getJobExecutionId())
        .withMatchedProfileId("df7bf522-66e1-4b52-9d48-abd739f37934")
        .withOrder(27)
        .withGeneration(2)
        .withRecordType(Record.RecordType.MARC)
        .withState(Record.State.OLD)
        .withExternalIdsHolder(new ExternalIdsHolder()
          .withInstanceId("e50e9535-091c-451e-82b7-d078b7c4770f")),
      new Record()
        .withId(id4)
        .withMatchedId(id4)
        .withSnapshotId(snapshot1.getJobExecutionId())
        .withMatchedProfileId("f9926e86-883b-4455-a807-fc5eeb9a951a")
        .withOrder(1)
        .withGeneration(1)
        .withRecordType(Record.RecordType.MARC)
        .withState(Record.State.DRAFT)
        .withExternalIdsHolder(new ExternalIdsHolder()
          .withInstanceId("c1d3be12-ecec-4fab-9237-baf728575185")),
      new Record()
        .withId(id5)
        .withMatchedId(id4)
        .withSnapshotId(snapshot2.getJobExecutionId())
        .withMatchedProfileId("df7bf522-66e1-4b52-9d48-abd739f37934")
        .withOrder(101)
        .withGeneration(2)
        .withRecordType(Record.RecordType.MARC)
        .withState(Record.State.ACTUAL)
        .withExternalIdsHolder(new ExternalIdsHolder()
          .withInstanceId("c1d3be12-ecec-4fab-9237-baf728575185"))
    };
  }
  
}