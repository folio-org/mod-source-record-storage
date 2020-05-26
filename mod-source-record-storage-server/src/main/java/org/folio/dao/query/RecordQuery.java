package org.folio.dao.query;


import static org.folio.dao.impl.LBRecordDaoImpl.GENERATION_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.INSTANCE_ID_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.MATCHED_ID_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.ORDER_IN_FILE_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.RECORD_TYPE_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.SNAPSHOT_ID_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.STATE_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.SUPPRESS_DISCOVERY_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.CREATED_BY_USER_ID_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.CREATED_DATE_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.RECORDS_TABLE_NAME;
import static org.folio.dao.util.DaoUtil.UPDATED_BY_USER_ID_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.UPDATED_DATE_COLUMN_NAME;

import org.folio.dao.query.Metamodel.Property;
import org.folio.rest.jaxrs.model.Record;

@Metamodel(
  entity = Record.class,
  table = RECORDS_TABLE_NAME,
  properties = {
    @Property(path = "id", column = ID_COLUMN_NAME),
    @Property(path = "snapshotId", column = SNAPSHOT_ID_COLUMN_NAME),
    @Property(path = "matchedId", column = MATCHED_ID_COLUMN_NAME),
    @Property(path = "generation", column = GENERATION_COLUMN_NAME),
    @Property(path = "recordType", column = RECORD_TYPE_COLUMN_NAME),
    @Property(path = "order", column = ORDER_IN_FILE_COLUMN_NAME),
    @Property(path = "externalIdsHolder.instanceId", column = INSTANCE_ID_COLUMN_NAME),
    @Property(path = "additionalInfo.suppressDiscovery", column = SUPPRESS_DISCOVERY_COLUMN_NAME),
    @Property(path = "state", column = STATE_COLUMN_NAME),
    @Property(path = "metadata.createdByUserId", column = CREATED_BY_USER_ID_COLUMN_NAME),
    @Property(path = "metadata.createdDate", column = CREATED_DATE_COLUMN_NAME),
    @Property(path = "metadata.updatedByUserId", column = UPDATED_BY_USER_ID_COLUMN_NAME),
    @Property(path = "metadata.updatedDate", column = UPDATED_DATE_COLUMN_NAME),
  }
)
public class RecordQuery extends AbstractEntityQuery<RecordQuery> {

  public static RecordQuery query() {
    return new RecordQuery();
  }

}