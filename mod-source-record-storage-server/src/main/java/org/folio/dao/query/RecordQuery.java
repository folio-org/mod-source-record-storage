package org.folio.dao.query;

import static org.folio.dao.impl.LBRecordDaoImpl.CREATED_BY_USER_ID_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.CREATED_DATE_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.GENERATION_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.INSTANCE_ID_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.MATCHED_ID_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.MATCHED_PROFILE_ID_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.ORDER_IN_FILE_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.RECORD_TYPE_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.SNAPSHOT_ID_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.STATE_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.SUPPRESS_DISCOVERY_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.UPDATED_BY_USER_ID_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.UPDATED_DATE_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.folio.rest.jaxrs.model.Record;

public class RecordQuery extends AbstractEntityQuery {

  private static final Map<String, String> ptc = new HashMap<>();

  static {
    ptc.put("id", ID_COLUMN_NAME);
    ptc.put("snapshotId", SNAPSHOT_ID_COLUMN_NAME);
    ptc.put("matchedProfileId", MATCHED_PROFILE_ID_COLUMN_NAME);
    ptc.put("matchedId", MATCHED_ID_COLUMN_NAME);
    ptc.put("generation", GENERATION_COLUMN_NAME);
    ptc.put("recordType", RECORD_TYPE_COLUMN_NAME);
    ptc.put("order", ORDER_IN_FILE_COLUMN_NAME);
    ptc.put("externalIdsHolder.instanceId", INSTANCE_ID_COLUMN_NAME);
    ptc.put("additionalInfo.suppressDiscovery", SUPPRESS_DISCOVERY_COLUMN_NAME);
    ptc.put("state", STATE_COLUMN_NAME);
    ptc.put("metadata.createdByUserId", CREATED_BY_USER_ID_COLUMN_NAME);
    ptc.put("metadata.createdDate", CREATED_DATE_COLUMN_NAME);
    ptc.put("metadata.updatedByUserId", UPDATED_BY_USER_ID_COLUMN_NAME);
    ptc.put("metadata.updatedDate", UPDATED_DATE_COLUMN_NAME);
  }

  private RecordQuery() {
    super(ImmutableMap.copyOf(ptc), Record.class);
  }

  public static RecordQuery query() {
    return new RecordQuery();
  }

}