package org.folio.dao.impl;

import static org.folio.dao.util.DaoUtil.DATE_FORMATTER;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.RECORDS_TABLE_NAME;

import java.time.Instant;
import java.util.Date;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.folio.dao.AbstractBeanDao;
import org.folio.dao.LBRecordDao;
import org.folio.dao.filter.RecordFilter;
import org.folio.dao.util.ColumnBuilder;
import org.folio.rest.jaxrs.model.AdditionalInfo;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.RecordType;
import org.folio.rest.jaxrs.model.Record.State;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.springframework.stereotype.Component;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
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
//   <column name="createddate" type="timestamptz"></column>
//   <column name="updatedbyuserid" type="uuid"></column>
//   <column name="updateddate" type="timestamptz"></column>
// </createTable>
@Component
public class LBRecordDaoImpl extends AbstractBeanDao<Record, RecordCollection, RecordFilter> implements LBRecordDao {

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

  @Override
  public String getTableName() {
    return RECORDS_TABLE_NAME;
  }

  @Override
  public String getId(Record record) {
    return record.getId();
  }

  @Override
  protected String getColumns() {
    return ColumnBuilder
      .of(ID_COLUMN_NAME)
      .append(MATCHED_ID_COLUMN_NAME)
      .append(SNAPSHOT_ID_COLUMN_NAME)
      .append(MATCHED_PROFILE_ID_COLUMN_NAME)
      .append(GENERATION_COLUMN_NAME)
      .append(ORDER_IN_FILE_COLUMN_NAME)
      .append(RECORD_TYPE_COLUMN_NAME)
      .append(STATE_COLUMN_NAME)
      .append(INSTANCE_ID_COLUMN_NAME)
      .append(SUPPRESS_DISCOVERY_COLUMN_NAME)
      .append(CREATED_BY_USER_ID_COLUMN_NAME)
      .append(CREATED_DATE_COLUMN_NAME)
      .append(UPDATED_BY_USER_ID_COLUMN_NAME)
      .append(UPDATED_DATE_COLUMN_NAME)
      .build();
  }

  @Override
  protected JsonArray toParams(Record record, boolean generateIdIfNotExists) {
    if (generateIdIfNotExists && StringUtils.isEmpty(record.getId())) {
      record.setId(UUID.randomUUID().toString());
    }
    if (StringUtils.isEmpty(record.getMatchedId())) {
      record.setMatchedId(record.getId());
    }
    JsonArray params = new JsonArray()
      .add(record.getId())
      .add(record.getMatchedId())
      .add(record.getSnapshotId())
      .add(record.getMatchedProfileId())
      .add(record.getGeneration())
      .add(record.getOrder())
      .add(record.getRecordType())
      .add(record.getState());
    if (record.getExternalIdsHolder() != null) {
      params.add(record.getExternalIdsHolder().getInstanceId());
    } else {
      params.addNull();
    }
    if (record.getAdditionalInfo() != null) {
      params.add(record.getAdditionalInfo().getSuppressDiscovery());
    } else {
      params.addNull();
    }
    if (record.getMetadata() != null) {
      params.add(record.getMetadata().getCreatedByUserId());
      if (record.getMetadata().getCreatedDate() != null) {
        params.add(DATE_FORMATTER.format(record.getMetadata().getCreatedDate()));
      }
      params.add(record.getMetadata().getUpdatedByUserId());
      if (record.getMetadata().getUpdatedDate() != null) {
        params.add(DATE_FORMATTER.format(record.getMetadata().getUpdatedDate()));
      }
    } else {
      params.addNull().addNull().addNull().addNull();
    }
    return params;
  }

  @Override
  protected RecordCollection toCollection(ResultSet resultSet) {
    return new RecordCollection()
      .withRecords(resultSet.getRows().stream().map(this::toBean).collect(Collectors.toList()))
      .withTotalRecords(resultSet.getNumRows());
  }

  @Override
  protected Record toBean(JsonObject result) {
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
    Instant createdDate = result.getInstant(CREATED_DATE_COLUMN_NAME);
    if (createdDate != null) {
      metadata.setCreatedDate(Date.from(createdDate));
    }
    String updatedByUserId = result.getString(UPDATED_BY_USER_ID_COLUMN_NAME);
    if (StringUtils.isNotEmpty(updatedByUserId)) {
      metadata.setUpdatedByUserId(updatedByUserId);
    }
    Instant updatedDate = result.getInstant(UPDATED_DATE_COLUMN_NAME);
    if (updatedDate != null) {
      metadata.setUpdatedDate(Date.from(updatedDate));
    }
    record.setMetadata(metadata);
    return record;
  }

}