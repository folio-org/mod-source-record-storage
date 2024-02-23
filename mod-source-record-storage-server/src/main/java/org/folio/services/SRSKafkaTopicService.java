package org.folio.services;

import static org.folio.RecordStorageKafkaTopic.MARC_BIB;
import static org.folio.kafka.KafkaTopicNameHelper.formatTopicName;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.kafka.services.KafkaEnvironmentProperties.environment;

import org.folio.kafka.services.KafkaTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

@Service
@PropertySource(value = "kafka.properties")
public class SRSKafkaTopicService {

  @Value("${di_parsed_records_chunk_saved.partitions}")
  private Integer diParsedRecordsChunkSavedPartitions;

  @Value("${di_srs_marc_bib_instance_hrid_set.partitions}")
  private Integer diSrsMarcBibInstanceHridSetPartitions;

  @Value("${di_marc_bib_record_matched.partitions}")
  private Integer diMarcBibRecordMatchedPartitions;

  @Value("${di_marc_bib_record_not_matched.partitions}")
  private Integer diMarcBibRecordNotMatchedPartitions;

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

  @Value("${di_marc_bib_record_modified_ready_for_post_processing.partitions}")
  private Integer diSrsMarcBibMatchedReadyForPostProcessingPartitions;

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
      new SRSKafkaTopic("DI_PARSED_RECORDS_CHUNK_SAVED", diParsedRecordsChunkSavedPartitions),
      new SRSKafkaTopic("DI_SRS_MARC_BIB_INSTANCE_HRID_SET", diSrsMarcBibInstanceHridSetPartitions),
      new SRSKafkaTopic("DI_SRS_MARC_BIB_RECORD_MATCHED", diMarcBibRecordMatchedPartitions),
      new SRSKafkaTopic("DI_SRS_MARC_BIB_RECORD_NOT_MATCHED", diMarcBibRecordNotMatchedPartitions),
      new SRSKafkaTopic("DI_SRS_MARC_AUTHORITY_RECORD_MATCHED", diMarcAuthorityRecordMatchedPartitions),
      new SRSKafkaTopic("DI_SRS_MARC_AUTHORITY_RECORD_NOT_MATCHED", diMarcAuthorityRecordNotMatchedPartitions),
      new SRSKafkaTopic("DI_SRS_MARC_AUTHORITY_RECORD_DELETED", diMarcAuthorityRecordDeletedPartitions),
      new SRSKafkaTopic("DI_SRS_MARC_HOLDINGS_HOLDING_HRID_SET", diSrsMarcHoldingsHridSetPartitions),
      new SRSKafkaTopic("DI_SRS_MARC_HOLDINGS_RECORD_MODIFIED_READY_FOR_POST_PROCESSING", diSrsMarcHoldingsModifiedReadyForPostProcessingPartitions),
      new SRSKafkaTopic("DI_SRS_MARC_HOLDINGS_RECORD_UPDATED", diMarcHoldingsRecordUpdatedPartitions),
      new SRSKafkaTopic("DI_SRS_MARC_BIB_RECORD_UPDATED", diMarcBibRecordUpdatedPartitions),
      new SRSKafkaTopic("DI_SRS_MARC_AUTHORITY_RECORD_MODIFIED_READY_FOR_POST_PROCESSING", diSrsMarcAuthorityModifiedReadyForPostProcessingPartitions),
      new SRSKafkaTopic("DI_SRS_MARC_BIB_RECORD_MATCHED_READY_FOR_POST_PROCESSING", diSrsMarcBibMatchedReadyForPostProcessingPartitions),
      new SRSKafkaTopic("DI_LOG_SRS_MARC_AUTHORITY_RECORD_CREATED", diLogSrsMarcAuthorityRecordCreatedPartitions),
      new SRSKafkaTopic("DI_LOG_SRS_MARC_AUTHORITY_RECORD_UPDATED", diLogSrsMarcAuthorityRecordUpdatedPartitions),
      new SRSKafkaTopic("DI_SRS_MARC_HOLDINGS_RECORD_MATCHED", diMarcHoldingsMatchedPartitions),
      new SRSKafkaTopic("DI_SRS_MARC_HOLDINGS_RECORD_NOT_MATCHED", diMarcHoldingsNotMatchedPartitions),
      new SRSKafkaTopic("DI_SRS_MARC_AUTHORITY_RECORD_UPDATED", diMarcAuthorityRecordUpdatedPartitions)
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
