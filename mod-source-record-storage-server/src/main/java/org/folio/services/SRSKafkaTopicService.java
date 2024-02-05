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
    var marcBib = new SRSKafkaTopic("MARC_BIB", marcBibPartitions);
    var diParsedRecordChunkSaved = new SRSKafkaTopic("DI_PARSED_RECORDS_CHUNK_SAVED", diParsedRecordsChunkSavedPartitions);
    var diSrsMarcBibInstanceHridSet = new SRSKafkaTopic("DI_SRS_MARC_BIB_INSTANCE_HRID_SET",
      diSrsMarcBibInstanceHridSetPartitions);
    var diSrsMarcBibRecordModified = new SRSKafkaTopic("DI_SRS_MARC_BIB_RECORD_MODIFIED", diSrsMarcBibRecordModifiedPartitions);
    var diSrsMarcBibRecordModifiedReadyForPostProcessing = new SRSKafkaTopic("DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING",
      diSrsMarcBibRecordModifiedReadyForPostProcessingPartitions);
    var diMarcBibRecordMatched = new SRSKafkaTopic("DI_SRS_MARC_BIB_RECORD_MATCHED",
      diMarcBibRecordMatchedPartitions);
    var diMarcBibRecordNotMatched = new SRSKafkaTopic("DI_SRS_MARC_BIB_RECORD_NOT_MATCHED",
      diMarcBibRecordNotMatchedPartitions);
    var diMarcAuthorityRecordMatched = new SRSKafkaTopic("DI_SRS_MARC_AUTHORITY_RECORD_MATCHED",
      diMarcAuthorityRecordMatchedPartitions);

    var diMarcAuthorityRecordNotMatched = new SRSKafkaTopic("DI_SRS_MARC_AUTHORITY_RECORD_NOT_MATCHED",
      diMarcAuthorityRecordNotMatchedPartitions);

    var diMarcAuthorityRecordDeleted = new SRSKafkaTopic("DI_SRS_MARC_AUTHORITY_RECORD_DELETED",
      diMarcAuthorityRecordDeletedPartitions);

    return new SRSKafkaTopic[] {marcBib, diParsedRecordChunkSaved,
                                diSrsMarcBibInstanceHridSet, diSrsMarcBibRecordModified,
                                diSrsMarcBibRecordModifiedReadyForPostProcessing, diMarcBibRecordMatched,
                                diMarcBibRecordNotMatched, diMarcAuthorityRecordMatched,
                                diMarcAuthorityRecordNotMatched, diMarcAuthorityRecordDeleted};
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
