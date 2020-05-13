package org.folio.dao.impl;

import static org.folio.dao.util.DaoUtil.DATE_FORMATTER;
import static org.folio.dao.util.DaoUtil.GET_BY_WHERE_SQL_TEMPLATE;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.RECORDS_TABLE_NAME;

import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.folio.dao.AbstractEntityDao;
import org.folio.dao.LBRecordDao;
import org.folio.dao.query.RecordQuery;
import org.folio.dao.util.ColumnBuilder;
import org.folio.dao.util.DaoUtil;
import org.folio.rest.jaxrs.model.AdditionalInfo;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.RecordType;
import org.folio.rest.jaxrs.model.Record.State;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.springframework.stereotype.Component;

import io.vertx.core.Future;
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
public class LBRecordDaoImpl extends AbstractEntityDao<Record, RecordCollection, RecordQuery> implements LBRecordDao {

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
  public Future<Optional<Record>> getByMatchedId(String matchedId, String tenantId) {
    String sql = String.format(GET_BY_WHERE_SQL_TEMPLATE, getColumns(), getTableName(), MATCHED_ID_COLUMN_NAME, matchedId);
    log.info("Attempting get by matched id: {}", sql);
    return select(sql, tenantId);
  }

  @Override
  public Future<Optional<Record>> getByInstanceId(String instanceId, String tenantId) {
    String sql = String.format(GET_BY_WHERE_SQL_TEMPLATE, getColumns(), getTableName(), INSTANCE_ID_COLUMN_NAME, instanceId);
    log.info("Attempting get by instance id: {}", sql);
    return select(sql, tenantId);
  }

  @Override
  public String getTableName() {
    return RECORDS_TABLE_NAME;
  }

  @Override
  public String getColumns() {
    return ColumnBuilder
      .of(ID_COLUMN_NAME)
      .append(SNAPSHOT_ID_COLUMN_NAME)
      .append(MATCHED_PROFILE_ID_COLUMN_NAME)
      .append(MATCHED_ID_COLUMN_NAME)
      .append(GENERATION_COLUMN_NAME)
      .append(RECORD_TYPE_COLUMN_NAME)
      .append(INSTANCE_ID_COLUMN_NAME)
      .append(STATE_COLUMN_NAME)
      .append(ORDER_IN_FILE_COLUMN_NAME)
      .append(SUPPRESS_DISCOVERY_COLUMN_NAME)
      .append(CREATED_BY_USER_ID_COLUMN_NAME)
      .append(CREATED_DATE_COLUMN_NAME)
      .append(UPDATED_BY_USER_ID_COLUMN_NAME)
      .append(UPDATED_DATE_COLUMN_NAME)
      .build();
  }

  @Override
  public String getId(Record record) {
    return record.getId();
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
      .add(record.getSnapshotId())
      .add(record.getMatchedProfileId())
      .add(record.getMatchedId())
      .add(record.getGeneration())
      .add(record.getRecordType());
    if (Objects.nonNull(record.getExternalIdsHolder())) {
      params.add(record.getExternalIdsHolder().getInstanceId());
    } else {
      params.addNull();
    }
    params
      .add(record.getState())
      .add(record.getOrder());
    if (Objects.nonNull(record.getAdditionalInfo())) {
      params.add(record.getAdditionalInfo().getSuppressDiscovery());
    } else {
      params.addNull();
    }
    if (Objects.nonNull(record.getMetadata())) {
      params.add(record.getMetadata().getCreatedByUserId());
      if (Objects.nonNull(record.getMetadata().getCreatedDate())) {
        params.add(DATE_FORMATTER.format(record.getMetadata().getCreatedDate()));
      } else {
        params.addNull();
      }
      params.add(record.getMetadata().getUpdatedByUserId());
      if (Objects.nonNull(record.getMetadata().getUpdatedDate())) {
        params.add(DATE_FORMATTER.format(record.getMetadata().getUpdatedDate()));
      } else {
        params.addNull();
      }
    } else {
      params.addNull().addNull().addNull().addNull();
    }
    return params;
  }

  @Override
  protected RecordCollection toCollection(ResultSet resultSet) {
    return toEmptyCollection(resultSet)
      .withRecords(resultSet.getRows().stream().map(this::toEntity).collect(Collectors.toList()));
  }

  @Override
  protected RecordCollection toEmptyCollection(ResultSet resultSet) {
    return new RecordCollection()
      .withRecords(Collections.emptyList())
      .withTotalRecords(DaoUtil.getTotalRecords(resultSet));
  }

  @Override
  protected Record toEntity(JsonObject result) {
    Record record = new Record()
      .withId(result.getString(ID_COLUMN_NAME))
      .withSnapshotId(result.getString(SNAPSHOT_ID_COLUMN_NAME))
      .withMatchedProfileId(result.getString(MATCHED_PROFILE_ID_COLUMN_NAME))
      .withMatchedId(result.getString(MATCHED_ID_COLUMN_NAME))
      .withGeneration(result.getInteger(GENERATION_COLUMN_NAME))
      .withRecordType(RecordType.valueOf(result.getString(RECORD_TYPE_COLUMN_NAME)));
    String instanceId = result.getString(INSTANCE_ID_COLUMN_NAME);
    if (StringUtils.isNotEmpty(instanceId)) {
      ExternalIdsHolder externalIdHolder = new ExternalIdsHolder();
      externalIdHolder.setInstanceId(instanceId);
      record.setExternalIdsHolder(externalIdHolder);
    }
    record
      .withState(State.valueOf(result.getString(STATE_COLUMN_NAME)))
      .withOrder(result.getInteger(ORDER_IN_FILE_COLUMN_NAME));
    if (result.containsKey(SUPPRESS_DISCOVERY_COLUMN_NAME)) {
      AdditionalInfo additionalInfo = new AdditionalInfo();
      additionalInfo.setSuppressDiscovery(result.getBoolean(SUPPRESS_DISCOVERY_COLUMN_NAME));
      record.setAdditionalInfo(additionalInfo);
    }
    Metadata metadata = new Metadata();
    String createdByUserId = result.getString(CREATED_BY_USER_ID_COLUMN_NAME);
    if (StringUtils.isNotEmpty(createdByUserId)) {
      metadata.setCreatedByUserId(createdByUserId);
    }
    Instant createdDate = result.getInstant(CREATED_DATE_COLUMN_NAME);
    if (Objects.nonNull(createdDate)) {
      metadata.setCreatedDate(Date.from(createdDate));
    }
    String updatedByUserId = result.getString(UPDATED_BY_USER_ID_COLUMN_NAME);
    if (StringUtils.isNotEmpty(updatedByUserId)) {
      metadata.setUpdatedByUserId(updatedByUserId);
    }
    Instant updatedDate = result.getInstant(UPDATED_DATE_COLUMN_NAME);
    if (Objects.nonNull(updatedDate)) {
      metadata.setUpdatedDate(Date.from(updatedDate));
    }
    record.setMetadata(metadata);
    return record;
  }

  @Override
  protected Record toEntity(JsonArray row) {
    Record record = new Record()
      .withId(row.getString(0))
      .withSnapshotId(row.getString(1))
      .withMatchedProfileId(row.getString(2))
      .withMatchedId(row.getString(3))
      .withGeneration(row.getInteger(4))
      .withRecordType(RecordType.valueOf(row.getString(5)));
    String instanceId = row.getString(6);
    if (StringUtils.isNotEmpty(instanceId)) {
      ExternalIdsHolder externalIdHolder = new ExternalIdsHolder();
      externalIdHolder.setInstanceId(instanceId);
      record.setExternalIdsHolder(externalIdHolder);
    }
    record
      .withState(State.valueOf(row.getString(7)))
      .withOrder(row.getInteger(8));
    Boolean suppressDiscovery = row.getBoolean(9);
    if (Objects.nonNull(suppressDiscovery)) {
      AdditionalInfo additionalInfo = new AdditionalInfo();
      additionalInfo.setSuppressDiscovery(suppressDiscovery);
      record.setAdditionalInfo(additionalInfo);
    }
    record.setMetadata(DaoUtil.metadataFromJsonArray(row, new int[] { 10, 11, 12, 13 }));
    return record;
  }

}