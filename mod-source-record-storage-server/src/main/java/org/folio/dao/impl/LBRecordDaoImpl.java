package org.folio.dao.impl;

import static com.fasterxml.jackson.databind.util.StdDateFormat.DATE_FORMAT_STR_ISO8601;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.RECORDS_TABLE_NAME;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.folio.dao.LBRecordDao;
import org.folio.dao.PostgresClientFactory;
import org.folio.dao.util.ColumnsBuilder;
import org.folio.dao.util.ValuesBuilder;
import org.folio.rest.jaxrs.model.AdditionalInfo;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.RecordType;
import org.folio.rest.jaxrs.model.Record.State;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.persist.PostgresClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.ResultSet;

// <createTable tableName="records_lb">
//   <column name="id" type="uuid">
//     <constraints primaryKey="true" nullable="false"/>
//   </column>
//   <column name="snapshotid" type="uuid">
//     <constraints nullable="false"/>
//   </column>
//   <column name="matchedprofileid" type="uuid">
//     <constraints nullable="false"/>
//   </column>
//   <column name="matchedid" type="uuid">
//     <constraints nullable="false"/>
//   </column>
//   <column name="generation" type="integer">
//     <constraints nullable="false"/>
//   </column>
//   <column name="recordtype" type="${database.defaultSchemaName}.record_type">
//     <constraints nullable="false"/>
//   </column>
//   <column name="instanceid" type="uuid"></column>
//   <column name="state" type="${database.defaultSchemaName}.record_state_type">
//     <constraints nullable="false"/>
//   </column>
//   <column name="orderinfile" type="integer"></column>
//   <column name="suppressdiscovery" type="boolean"></column>
//   <column name="createdbyuserid" type="uuid"></column>
//   <column name="createddate" type="timestamp"></column>
//   <column name="updatedbyuserid" type="uuid"></column>
//   <column name="updateddate" type="timestamp"></column>
// </createTable>
@Component
public class LBRecordDaoImpl implements LBRecordDao {

  private static final Logger LOG = LoggerFactory.getLogger(LBRecordDaoImpl.class);

  public static final String MATCHED_ID_COLUMN_NAME = "matchedid";
  public static final String SNAPSHOT_ID_COLUMN_NAME = "snapshotid";
  public static final String MATCHED_PROFILE_ID_COLUMN_NAME = "matchedprofileid";
  public static final String GENERATION_COLUMN_NAME = "generation";
  public static final String ORDER_IN_FILE_COLUMN_NAME = "orderinfile";
  public static final String RECORD_TYPE_COLUMN_NAME = "recordtype";
  public static final String STATE_COLUMN_NAME = "state";
  public static final String INSTANCE_ID_COLUMN_NAME = "instanceid";
  public static final String SUPPRESS_DISCOVERY_COLUMN_NAME = "suppressdiscovery";
  public static final String CREATED_BY_USER_ID_COLUMN_NAME = "createdbyuserid";
  public static final String CREATED_DATE_COLUMN_NAME = "createddate";
  public static final String UPDATED_BY_USER_ID_COLUMN_NAME = "updatedbyuserid";
  public static final String UPDATED_DATE_COLUMN_NAME = "updateddate";

  private final PostgresClientFactory pgClientFactory;

  @Autowired 
  public LBRecordDaoImpl(PostgresClientFactory pgClientFactory) {
    this.pgClientFactory = pgClientFactory;
  }

  @Override
  public Logger getLogger() {
    return LOG;
  }

  @Override
  public PostgresClient getPostgresClient(String tenantId) {
    return pgClientFactory.createInstance(tenantId);
  }

  @Override
  public String getTableName() {
    return RECORDS_TABLE_NAME;
  }

  @Override
  public String getId(Record record) {
    return record.getId();
  }

  @Override
  public String toColumns(Record record) {
    ColumnsBuilder columnBuilder = ColumnsBuilder
      .of(String.format("%s,%s", ID_COLUMN_NAME, MATCHED_ID_COLUMN_NAME))
      .append(record.getSnapshotId(), SNAPSHOT_ID_COLUMN_NAME)
      .append(record.getMatchedProfileId(), MATCHED_PROFILE_ID_COLUMN_NAME)
      .append(record.getGeneration(), GENERATION_COLUMN_NAME)
      .append(record.getOrder(), ORDER_IN_FILE_COLUMN_NAME)
      .append(record.getRecordType(), RECORD_TYPE_COLUMN_NAME)
      .append(record.getState(), STATE_COLUMN_NAME);
    if (record.getExternalIdsHolder() != null) {
      columnBuilder
        .append(record.getExternalIdsHolder().getInstanceId(), INSTANCE_ID_COLUMN_NAME);
    }
    if (record.getAdditionalInfo() != null) {
      columnBuilder
        .append(record.getAdditionalInfo().getSuppressDiscovery(), SUPPRESS_DISCOVERY_COLUMN_NAME);
    }
    if (record.getMetadata() != null) {
      columnBuilder
        .append(record.getMetadata().getCreatedByUserId(), CREATED_BY_USER_ID_COLUMN_NAME);
      columnBuilder
        .append(record.getMetadata().getCreatedDate(), CREATED_DATE_COLUMN_NAME);
      columnBuilder
        .append(record.getMetadata().getUpdatedByUserId(), UPDATED_BY_USER_ID_COLUMN_NAME);
      columnBuilder
        .append(record.getMetadata().getUpdatedDate(), UPDATED_DATE_COLUMN_NAME);
    }
    return columnBuilder.build();
  }

  @Override
  public String toValues(Record record, boolean generateIdIfNotExists) {
    if (generateIdIfNotExists && StringUtils.isEmpty(record.getId())) {
      record.setId(UUID.randomUUID().toString());
    }
    if (StringUtils.isEmpty(record.getMatchedId())) {
      record.setMatchedId(record.getId());
    }
    ValuesBuilder valuesBuilder = ValuesBuilder.of(record.getId())
      .append(record.getMatchedId())
      .append(record.getSnapshotId())
      .append(record.getMatchedProfileId())
      .append(record.getGeneration())
      .append(record.getOrder());
    if (record.getRecordType() != null) {
      valuesBuilder
        .append(record.getRecordType().toString());
    }
    if (record.getState() != null) {
      valuesBuilder
        .append(record.getState().toString());
    }
    if (record.getExternalIdsHolder() != null) {
      valuesBuilder
        .append(record.getExternalIdsHolder().getInstanceId());
    }
    if (record.getAdditionalInfo() != null) {
      valuesBuilder
        .append(record.getAdditionalInfo().getSuppressDiscovery());
    }
    if (record.getMetadata() != null) {
      valuesBuilder
        .append(record.getMetadata().getCreatedByUserId());
      valuesBuilder
        .append(record.getMetadata().getCreatedDate());
      valuesBuilder
        .append(record.getMetadata().getUpdatedByUserId());
      valuesBuilder
        .append(record.getMetadata().getUpdatedDate());
    }
    return valuesBuilder.build();
  }

  @Override
  public RecordCollection toCollection(ResultSet resultSet) {
    return new RecordCollection()
      .withRecords(resultSet.getRows().stream().map(this::toBean).collect(Collectors.toList()))
      .withTotalRecords(resultSet.getNumRows());
  }

  @Override
  public Record toBean(JsonObject result) {
    Record record = new Record()
      .withId(result.getString(ID_COLUMN_NAME))
      .withMatchedId(result.getString(MATCHED_ID_COLUMN_NAME))
      .withSnapshotId(result.getString(SNAPSHOT_ID_COLUMN_NAME))
      .withMatchedProfileId(result.getString(MATCHED_PROFILE_ID_COLUMN_NAME))
      .withGeneration(result.getInteger(GENERATION_COLUMN_NAME))
      .withRecordType(RecordType.valueOf(result.getString(RECORD_TYPE_COLUMN_NAME)))
      .withState(State.valueOf(result.getString(STATE_COLUMN_NAME)));
    
    if (result.containsKey(ORDER_IN_FILE_COLUMN_NAME)) {
      record.setOrder(result.getInteger(ORDER_IN_FILE_COLUMN_NAME));
    }

    ExternalIdsHolder externalIdHolder = new ExternalIdsHolder();
    String instanceId = result.getString(INSTANCE_ID_COLUMN_NAME);
    if (StringUtils.isNotEmpty(instanceId)) {
      externalIdHolder.setInstanceId(instanceId);
    }
    record.setExternalIdsHolder(externalIdHolder);

    AdditionalInfo additionalInfo = new AdditionalInfo();
    if (result.containsKey(SUPPRESS_DISCOVERY_COLUMN_NAME)) {
      additionalInfo.setSuppressDiscovery(result.getBoolean(SUPPRESS_DISCOVERY_COLUMN_NAME));
    }
    record.setAdditionalInfo(additionalInfo);

    Metadata metadata = new Metadata();
    String createdByUserId = result.getString(CREATED_BY_USER_ID_COLUMN_NAME);
    if (StringUtils.isNotEmpty(createdByUserId)) {
      metadata.setCreatedByUserId(createdByUserId);
    }
    String createdDate = result.getString(CREATED_DATE_COLUMN_NAME);
    if (StringUtils.isNotEmpty(createdDate)) {
      try {
        metadata.setCreatedDate(new SimpleDateFormat(DATE_FORMAT_STR_ISO8601).parse(createdDate));
      } catch (ParseException e) {
        LOG.error(e.getMessage(), e.getCause());
      }
    }
    String updatedByUserId = result.getString(UPDATED_BY_USER_ID_COLUMN_NAME);
    if (StringUtils.isNotEmpty(updatedByUserId)) {
      metadata.setUpdatedByUserId(updatedByUserId);
    }
    String updatedDate = result.getString(UPDATED_DATE_COLUMN_NAME);
    if (StringUtils.isNotEmpty(updatedDate)) {
      try {
        metadata.setUpdatedDate(new SimpleDateFormat(DATE_FORMAT_STR_ISO8601).parse(updatedDate));
      } catch (ParseException e) {
        LOG.error(e.getMessage(), e.getCause());
      }
    }
    record.setMetadata(metadata);
    return record;
  }

}