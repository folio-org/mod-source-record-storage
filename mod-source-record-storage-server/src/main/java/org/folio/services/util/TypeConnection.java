package org.folio.services.util;

import org.folio.dao.util.RecordType;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.Record;

public enum TypeConnection {

  MARC_BIB(RecordType.MARC_BIB, EntityType.MARC_BIBLIOGRAPHIC, Record.RecordType.MARC_BIB, EntityType.INSTANCE),
  MARC_HOLDINGS(RecordType.MARC_HOLDING, EntityType.MARC_HOLDINGS, Record.RecordType.MARC_HOLDING, EntityType.HOLDINGS);

  private final RecordType dbType;
  private final EntityType marcType;
  private final Record.RecordType recordType;
  private final EntityType externalType;

  TypeConnection(RecordType dbType, EntityType marcType, Record.RecordType recordType,
                 EntityType externalType) {

    this.dbType = dbType;
    this.marcType = marcType;
    this.recordType = recordType;
    this.externalType = externalType;
  }

  public EntityType getMarcType() {
    return marcType;
  }

  public Record.RecordType getRecordType() {
    return recordType;
  }

  public EntityType getExternalType() {
    return externalType;
  }

  public RecordType getDbType() {
    return dbType;
  }
}
