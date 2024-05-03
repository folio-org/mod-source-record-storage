package org.folio.services;

import static org.folio.RecordStorageKafkaTopic.MARC_BIB;
import static org.folio.kafka.KafkaTopicNameHelper.formatTopicName;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.kafka.services.KafkaEnvironmentProperties.environment;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_LOG_SRS_MARC_AUTHORITY_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_LOG_SRS_MARC_AUTHORITY_RECORD_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_PARSED_RECORDS_CHUNK_SAVED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_DELETED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_MODIFIED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_NOT_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_HOLDINGS_HOLDING_HRID_SET;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_HOLDINGS_RECORD_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_HOLDINGS_RECORD_MODIFIED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_HOLDINGS_RECORD_NOT_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_HOLDINGS_RECORD_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING;

import org.folio.kafka.services.KafkaTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

@Service
@PropertySource(value = "kafka.properties")
public class SRSKafkaTopicService {

  @Value("${di_parsed_records_chunk_saved.partitions}")
  private Integer diParsedRecordsChunkSavedPartitions;

  @Value("${di_srs_marc_bib_record_modified_ready_for_post_processing.partitions}")
  private Integer diSrsMarcBibRecordModifiedReadyForPostProcessingPartitions;

  @Value("${di_marc_authority_record_matched.partitions}")
  private Integer diMarcAuthorityRecordMatchedPartitions;

  @Value("${di_marc_authority_record_not_matched.partitions}")
  private Integer diMarcAuthorityRecordNotMatchedPartitions;

  @Value("${di_marc_authority_record_deleted.partitions}")
  private Integer diMarcAuthorityRecordDeletedPartitions;

  @Value("${di_marc_holdings_hrid_set.partitions}")
  private Integer diSrsMarcHoldingsHridSetPartitions;

  @Value("${di_marc_holdings_record_modified_ready_for_post_processing.partitions}")
  private Integer diSrsMarcHoldingsModifiedReadyForPostProcessingPartitions;

  @Value("${di_marc_holdings_record_updated.partitions}")
  private Integer diMarcHoldingsRecordUpdatedPartitions;

  @Value("${di_marc_bib_record_updated.partitions}")
  private Integer diMarcBibRecordUpdatedPartitions;

  @Value("${di_marc_authority_record_modified_ready_for_post_processing.partitions}")
  private Integer diSrsMarcAuthorityModifiedReadyForPostProcessingPartitions;

  @Value("${di_logs_srs_marc_authority_record_created.partitions}")
  private Integer diLogSrsMarcAuthorityRecordCreatedPartitions;

  @Value("${di_logs_srs_marc_authority_record_updated.partitions}")
  private Integer diLogSrsMarcAuthorityRecordUpdatedPartitions;

  @Value("${di_marc_holdings_matched.partitions}")
  private Integer diMarcHoldingsMatchedPartitions;

  @Value("${di_marc_holdings_not_matched.partitions}")
  private Integer diMarcHoldingsNotMatchedPartitions;

  @Value("${di_marc_authority_record_updated.partitions}")
  private Integer diMarcAuthorityRecordUpdatedPartitions;

  public KafkaTopic[] createTopicObjects() {
    return new KafkaTopic[] {
      MARC_BIB,
      new SRSKafkaTopic(DI_PARSED_RECORDS_CHUNK_SAVED.value(), diParsedRecordsChunkSavedPartitions),
      new SRSKafkaTopic(DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING.value(), diSrsMarcBibRecordModifiedReadyForPostProcessingPartitions),
      new SRSKafkaTopic(DI_SRS_MARC_AUTHORITY_RECORD_MATCHED.value(), diMarcAuthorityRecordMatchedPartitions),
      new SRSKafkaTopic(DI_SRS_MARC_AUTHORITY_RECORD_NOT_MATCHED.value(), diMarcAuthorityRecordNotMatchedPartitions),
      new SRSKafkaTopic(DI_SRS_MARC_AUTHORITY_RECORD_DELETED.value(), diMarcAuthorityRecordDeletedPartitions),
      new SRSKafkaTopic(DI_SRS_MARC_HOLDINGS_HOLDING_HRID_SET.value(), diSrsMarcHoldingsHridSetPartitions),
      new SRSKafkaTopic(DI_SRS_MARC_HOLDINGS_RECORD_MODIFIED_READY_FOR_POST_PROCESSING.value(), diSrsMarcHoldingsModifiedReadyForPostProcessingPartitions),
      new SRSKafkaTopic(DI_SRS_MARC_HOLDINGS_RECORD_UPDATED.value(), diMarcHoldingsRecordUpdatedPartitions),
      new SRSKafkaTopic(DI_SRS_MARC_BIB_RECORD_UPDATED.value(), diMarcBibRecordUpdatedPartitions),
      new SRSKafkaTopic(DI_SRS_MARC_AUTHORITY_RECORD_MODIFIED_READY_FOR_POST_PROCESSING.value(), diSrsMarcAuthorityModifiedReadyForPostProcessingPartitions),
      new SRSKafkaTopic(DI_LOG_SRS_MARC_AUTHORITY_RECORD_CREATED.value(), diLogSrsMarcAuthorityRecordCreatedPartitions),
      new SRSKafkaTopic(DI_LOG_SRS_MARC_AUTHORITY_RECORD_UPDATED.value(), diLogSrsMarcAuthorityRecordUpdatedPartitions),
      new SRSKafkaTopic(DI_SRS_MARC_HOLDINGS_RECORD_MATCHED.value(), diMarcHoldingsMatchedPartitions),
      new SRSKafkaTopic(DI_SRS_MARC_HOLDINGS_RECORD_NOT_MATCHED.value(), diMarcHoldingsNotMatchedPartitions),
      new SRSKafkaTopic(DI_SRS_MARC_AUTHORITY_RECORD_UPDATED.value(), diMarcAuthorityRecordUpdatedPartitions)
    };
  }

  public static class SRSKafkaTopic implements KafkaTopic {

    private final String topic;
    private final int numPartitions;

    public SRSKafkaTopic(String topic, int numPartitions) {
      this.topic = topic;
      this.numPartitions = numPartitions;
    }

    @Override
    public String moduleName() {
      return "srs";
    }

    @Override
    public String topicName() {
      return topic;
    }

    @Override
    public int numPartitions() {
      return numPartitions;
    }

    @Override
    public String fullTopicName(String tenant) {
      return formatTopicName(environment(), getDefaultNameSpace(), tenant, topicName());
    }
  }
}
