package org.folio.services;

import static org.folio.kafka.KafkaTopicNameHelper.formatTopicName;
import static org.folio.kafka.services.KafkaEnvironmentProperties.environment;

import org.folio.kafka.services.KafkaTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

@Service
@PropertySource(value = "kafka.properties")
public class SRSKafkaTopicService {

  @Value("${marcBib.partitions}")
  private Integer marcBibPartitions;

  @Value("${di_parsed_records_chunk_saved.partitions}")
  private Integer diParsedRecordsChunkSavedPartitions;

  @Value("${di_srs_marc_bib_instance_hrid_set.partitions}")
  private Integer diSrsMarcBibInstanceHridSetPartitions;

  @Value("${di_srs_marc_bib_record_modified.partitions}")
  private Integer diSrsMarcBibRecordModifiedPartitions;

  @Value("${di_srs_marc_bib_record_modified_ready_for_post_processing.partitions}")
  private Integer diSrsMarcBibRecordModifiedReadyForPostProcessingPartitions;

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

  public KafkaTopic[] createTopicObjects() {
    return new SRSKafkaTopic[] {
      new SRSKafkaTopic("MARC_BIB", marcBibPartitions),
      new SRSKafkaTopic("DI_PARSED_RECORDS_CHUNK_SAVED", diParsedRecordsChunkSavedPartitions),
      new SRSKafkaTopic("DI_SRS_MARC_BIB_INSTANCE_HRID_SET", diSrsMarcBibInstanceHridSetPartitions),
      new SRSKafkaTopic("DI_SRS_MARC_BIB_RECORD_MODIFIED", diSrsMarcBibRecordModifiedPartitions),
      new SRSKafkaTopic("DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING",
        diSrsMarcBibRecordModifiedReadyForPostProcessingPartitions),
      new SRSKafkaTopic("DI_SRS_MARC_BIB_RECORD_MATCHED", diMarcBibRecordMatchedPartitions),
      new SRSKafkaTopic("DI_SRS_MARC_BIB_RECORD_NOT_MATCHED",
        diMarcBibRecordNotMatchedPartitions),
      new SRSKafkaTopic("DI_SRS_MARC_AUTHORITY_RECORD_MATCHED",
        diMarcAuthorityRecordMatchedPartitions),
      new SRSKafkaTopic("DI_SRS_MARC_AUTHORITY_RECORD_NOT_MATCHED",
        diMarcAuthorityRecordNotMatchedPartitions),
      new SRSKafkaTopic("DI_SRS_MARC_AUTHORITY_RECORD_DELETED",
      diMarcAuthorityRecordDeletedPartitions)};
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
      return formatTopicName(environment(), tenant, topicName());
    }
  }
}
