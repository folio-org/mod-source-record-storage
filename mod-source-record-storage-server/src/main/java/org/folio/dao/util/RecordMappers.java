package org.folio.dao.util;

import org.folio.rest.jooq.tables.pojos.RecordsLb;
import org.jooq.Record;

import java.util.function.Function;

public final class RecordMappers {

  private RecordMappers() {}

  public static Function<Record, RecordsLb> getDbRecordToRecordsLbMapper() {
    return jooqRecord -> {
      org.folio.rest.jooq.tables.pojos.RecordsLb pojo = new org.folio.rest.jooq.tables.pojos.RecordsLb();
      pojo.setId(jooqRecord.get(org.folio.rest.jooq.tables.RecordsLb.RECORDS_LB.ID));
      pojo.setSnapshotId(jooqRecord.get(org.folio.rest.jooq.tables.RecordsLb.RECORDS_LB.SNAPSHOT_ID));
      pojo.setMatchedId(jooqRecord.get(org.folio.rest.jooq.tables.RecordsLb.RECORDS_LB.MATCHED_ID));
      pojo.setGeneration(jooqRecord.get(org.folio.rest.jooq.tables.RecordsLb.RECORDS_LB.GENERATION));
      pojo.setRecordType(java.util.Arrays.stream(org.folio.rest.jooq.enums.RecordType.values())
        .filter(jooqRecord.get(org.folio.rest.jooq.tables.RecordsLb.RECORDS_LB.RECORD_TYPE)::equals)
        .findFirst()
        .orElse(null));
      pojo.setExternalId(jooqRecord.get(org.folio.rest.jooq.tables.RecordsLb.RECORDS_LB.EXTERNAL_ID));
      pojo.setState(java.util.Arrays.stream(org.folio.rest.jooq.enums.RecordState.values())
        .filter(jooqRecord.get(org.folio.rest.jooq.tables.RecordsLb.RECORDS_LB.STATE)::equals)
        .findFirst()
        .orElse(null));
      pojo.setLeaderRecordStatus(jooqRecord.get(org.folio.rest.jooq.tables.RecordsLb.RECORDS_LB.LEADER_RECORD_STATUS));
      pojo.setOrder(jooqRecord.get(org.folio.rest.jooq.tables.RecordsLb.RECORDS_LB.ORDER));
      pojo.setSuppressDiscovery(jooqRecord.get(org.folio.rest.jooq.tables.RecordsLb.RECORDS_LB.SUPPRESS_DISCOVERY));
      pojo.setCreatedByUserId(jooqRecord.get(org.folio.rest.jooq.tables.RecordsLb.RECORDS_LB.CREATED_BY_USER_ID));
      pojo.setCreatedDate(jooqRecord.get(org.folio.rest.jooq.tables.RecordsLb.RECORDS_LB.CREATED_DATE));
      pojo.setUpdatedByUserId(jooqRecord.get(org.folio.rest.jooq.tables.RecordsLb.RECORDS_LB.UPDATED_BY_USER_ID));
      pojo.setUpdatedDate(jooqRecord.get(org.folio.rest.jooq.tables.RecordsLb.RECORDS_LB.UPDATED_DATE));
      pojo.setExternalHrid(jooqRecord.get(org.folio.rest.jooq.tables.RecordsLb.RECORDS_LB.EXTERNAL_HRID));
      return pojo;
    };
  }
}
