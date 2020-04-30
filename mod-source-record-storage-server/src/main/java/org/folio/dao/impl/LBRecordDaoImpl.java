package org.folio.dao.impl;

import java.text.ParseException;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.folio.dao.LBRecordDao;
import org.folio.dao.PostgresClientFactory;
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

  private static final String TABLE_NAME = "records_lb";

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
    return TABLE_NAME;
  }

  @Override
  public String getId(Record record) {
    return record.getId();
  }

  @Override
  public String toColumns(Record record) {
    String columns = "id,matchedid";
    if (StringUtils.isNoneEmpty(record.getSnapshotId())) {
      columns += ",snapshotid";
    }
    if (StringUtils.isNoneEmpty(record.getMatchedProfileId())) {
      columns += ",matchedprofileid";
    }
    if (record.getGeneration() != null) {
      columns += ",generation";
    }
    if (record.getRecordType() != null) {
      columns += ",recordtype";
    }
    if (record.getExternalIdsHolder() != null) {
      if (StringUtils.isNoneEmpty(record.getExternalIdsHolder().getInstanceId())) {
        columns += ",instanceid";
      }
    }
    if (record.getState() != null) {
      columns += ",state";
    }
    if (record.getOrder() != null) {
      columns += ",orderinfile";
    }
    if (record.getAdditionalInfo() != null) {
      if (record.getAdditionalInfo().getSuppressDiscovery() != null) {
        columns += ",suppressdiscovery";
      }
    }
    if (record.getMetadata() != null) {
      if (StringUtils.isNotEmpty(record.getMetadata().getCreatedByUserId())) {
        columns += ",createdbyuserid";
      }
      if (record.getMetadata().getCreatedDate() != null) {
        columns += ",createddate";
      }
      if (StringUtils.isNotEmpty(record.getMetadata().getUpdatedByUserId())) {
        columns += ",updatedbyuserid";
      }
      if (record.getMetadata().getUpdatedDate() != null) {
        columns += ",updateddate";
      }
    }
    return columns;
  }

  @Override
  public String toValues(Record record, boolean generateIdIfNotExists) {
    if (generateIdIfNotExists && StringUtils.isEmpty(record.getId())) {
      String id = UUID.randomUUID().toString();
      record.setId(id);
      if (StringUtils.isEmpty(record.getMatchedId())) {
        record.setMatchedId(id);
      }
    }
    String values = String.format("'%s','%s'", record.getId(), record.getMatchedId());
    if (StringUtils.isNoneEmpty(record.getSnapshotId())) {
      values = String.format("%s,'%s'", values, record.getSnapshotId());
    }
    if (StringUtils.isNoneEmpty(record.getMatchedProfileId())) {
      values = String.format("%s,'%s'", values, record.getMatchedProfileId());
    }
    if (record.getGeneration() != null) {
      values = String.format("%s,%s", values, record.getGeneration());
    }
    if (record.getRecordType() != null) {
      values = String.format("%s,'%s'", values, record.getRecordType().toString());
    }
    if (record.getExternalIdsHolder() != null) {
      if (StringUtils.isNoneEmpty(record.getExternalIdsHolder().getInstanceId())) {
        
      }
    }
    if (record.getState() != null) {
      values = String.format("%s,'%s'", values, record.getState().toString());
    }
    if (record.getOrder() != null) {
      values = String.format("%s,%s", values, record.getOrder());
    }
    if (record.getAdditionalInfo() != null) {
      if (record.getAdditionalInfo().getSuppressDiscovery() != null) {
        values = String.format("%s,%s", values, record.getAdditionalInfo().getSuppressDiscovery());
      }
    }
    if (record.getMetadata() != null) {
      if (StringUtils.isNotEmpty(record.getMetadata().getCreatedByUserId())) {
        values = String.format("%s,'%s'", values, record.getMetadata().getCreatedByUserId());
      }
      if (record.getMetadata().getCreatedDate() != null) {
        values = String.format("%s,'%s'", values, ISO_8601_FORMAT.format(record.getMetadata().getCreatedDate()));
      }
      if (StringUtils.isNotEmpty(record.getMetadata().getUpdatedByUserId())) {
        values = String.format("%s,'%s'", values, record.getMetadata().getUpdatedByUserId());
      }
      if (record.getMetadata().getUpdatedDate() != null) {
        values = String.format("%s,'%s'", values, ISO_8601_FORMAT.format(record.getMetadata().getUpdatedDate()));
      }
    }
    return values;
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
      .withId(result.getString("id"))
      .withSnapshotId(result.getString("snapshotid"))
      .withMatchedProfileId(result.getString("matchedprofileid"))
      .withMatchedId(result.getString("matchedpid"))
      .withGeneration(result.getInteger("generation"))
      .withRecordType(RecordType.valueOf(result.getString("recordtype")))
      .withState(State.valueOf(result.getString("state")));
    
    if (result.containsKey("orderinfile")) {
      record.setOrder(result.getInteger("orderinfile"));
    }

    ExternalIdsHolder externalIdHolder = new ExternalIdsHolder();
    String instanceId = result.getString("instanceid");
    if (StringUtils.isNotEmpty(instanceId)) {
      externalIdHolder.setInstanceId(instanceId);
    }
    record.setExternalIdsHolder(externalIdHolder);

    AdditionalInfo additionalInfo = new AdditionalInfo();
    if (result.containsKey("suppressdiscovery")) {
      additionalInfo.setSuppressDiscovery(result.getBoolean("suppressdiscovery"));
    }
    record.setAdditionalInfo(additionalInfo);

    Metadata metadata = new Metadata();
    String createdByUserId = result.getString("createdbyuserid");
    if (StringUtils.isNotEmpty(createdByUserId)) {
      metadata.setCreatedByUserId(createdByUserId);
    }
    String createdDate = result.getString("createddate");
    if (StringUtils.isNotEmpty(createdDate)) {
      try {
        metadata.setCreatedDate(ISO_8601_FORMAT.parse(createdDate));
      } catch (ParseException e) {
        LOG.error(e.getMessage(), e.getCause());
      }
    }
    String updatedByUserId = result.getString("updatedbyuserid");
    if (StringUtils.isNotEmpty(updatedByUserId)) {
      metadata.setUpdatedByUserId(updatedByUserId);
    }
    String updatedDate = result.getString("updateddate");
    if (StringUtils.isNotEmpty(updatedDate)) {
      try {
        metadata.setUpdatedDate(ISO_8601_FORMAT.parse(updatedDate));
      } catch (ParseException e) {
        LOG.error(e.getMessage(), e.getCause());
      }
    }
    record.setMetadata(metadata);
    return record;
  }

}