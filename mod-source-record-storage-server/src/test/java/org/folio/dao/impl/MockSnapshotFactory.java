package org.folio.dao.impl;

import java.util.Date;

import org.folio.rest.jaxrs.model.Snapshot;

public class MockSnapshotFactory {

  private final static Date date1 = new Date();
  private final static Date date2 = new Date();
  private final static Date date3 = new Date();
  private final static Date date4 = new Date();

  public static Snapshot getMockSnapshot() {
    return new Snapshot()
      .withJobExecutionId("ac8f351c-8a2e-11ea-bc55-0242ac130003")
      .withStatus(Snapshot.Status.NEW);
  }

  public static Snapshot[] getMockSnapshots() {
    return new Snapshot[] {
      new Snapshot()
        .withJobExecutionId("17dfac11-1caf-4470-9ad1-d533f6360bdd")
        .withStatus(Snapshot.Status.PARSING_IN_PROGRESS)
        .withProcessingStartedDate(date1),
      new Snapshot()
        .withJobExecutionId("27dfac11-1caf-4470-9ad1-d533f6360bdd")
        .withStatus(Snapshot.Status.PARSING_IN_PROGRESS)
        .withProcessingStartedDate(date2),
      new Snapshot()
        .withJobExecutionId("37dfac11-1caf-4470-9ad1-d533f6360bdd")
        .withStatus(Snapshot.Status.PARSING_FINISHED)
        .withProcessingStartedDate(date3),
      new Snapshot()
        .withJobExecutionId("7644042a-805e-4465-b690-cafbc094d891")
        .withStatus(Snapshot.Status.DISCARDED)
        .withProcessingStartedDate(date4),
      new Snapshot()
        .withJobExecutionId("67dfac11-1caf-4470-9ad1-d533f6360bdd")
        .withStatus(Snapshot.Status.NEW),
    };
  }

}