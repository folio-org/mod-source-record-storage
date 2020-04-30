package org.folio.dao.filter;

import static org.folio.dao.BeanDao.ISO_8601_FORMAT;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.folio.rest.jaxrs.model.Record;

public class RecordFilter extends Record implements BeanFilter {

  public RecordFilter() {
    setState(null);
  }

  @Override
  public String toWhereClause() {
    List<String> statements = new ArrayList<>();
    if (StringUtils.isNotEmpty(getId())) {
      statements.add(String.format("id = '%s'", getId()));
    }
    if (StringUtils.isNotEmpty(getMatchedId())) {
      statements.add(String.format("matchedid = '%s'", getMatchedId()));
    }
    if (StringUtils.isNoneEmpty(getSnapshotId())) {
      statements.add(String.format("snapshotid = '%s'", getSnapshotId()));
    }
    if (StringUtils.isNotEmpty(getMatchedProfileId())) {
      statements.add(String.format("matchedprofileid = '%s'", getMatchedProfileId()));
    }
    if (getGeneration() != null) {
      statements.add(String.format("generation = %s", getGeneration()));
    }
    if (getRecordType() != null) {
      statements.add(String.format("recordtype = '%s'", getRecordType().toString()));
    }
    if (getExternalIdsHolder() != null) {
      if (StringUtils.isNoneEmpty(getExternalIdsHolder().getInstanceId())) {
        statements.add(String.format("instanceid = '%s'", getExternalIdsHolder().getInstanceId()));
      }
    }
    if (getState() != null) {
      statements.add(String.format("state = '%s'", getState().toString()));
    }
    if (getOrder() != null) {
      statements.add(String.format("orderinfile = %s", getOrder()));
    }
    if (getAdditionalInfo() != null) {
      if (getAdditionalInfo().getSuppressDiscovery() != null) {
        statements.add(String.format("suppressdiscovery = %s", getAdditionalInfo().getSuppressDiscovery()));
      }
    }
    if (getMetadata() != null) {
      if (StringUtils.isNotEmpty(getMetadata().getCreatedByUserId())) {
        statements.add(String.format("createdbyuserid = '%s'", getMetadata().getCreatedByUserId()));
      }
      if (getMetadata().getCreatedDate() != null) {
        statements.add(String.format("createddate = '%s'", ISO_8601_FORMAT.format(getMetadata().getCreatedDate())));
      }
      if (StringUtils.isNotEmpty(getMetadata().getUpdatedByUserId())) {
        statements.add(String.format("updatedbyuserid = '%s'", getMetadata().getUpdatedByUserId()));
      }
      if (getMetadata().getUpdatedDate() != null) {
        statements.add(String.format("updateddate = '%s'", ISO_8601_FORMAT.format(getMetadata().getUpdatedDate())));
      }
    }
    return statements.isEmpty() ? StringUtils.EMPTY : "WHERE " + String.join(" AND ", statements);
  }

}