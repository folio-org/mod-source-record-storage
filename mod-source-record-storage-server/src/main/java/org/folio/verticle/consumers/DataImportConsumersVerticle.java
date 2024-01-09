package org.folio.verticle.consumers;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_AUTHORITY_CREATED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_AUTHORITY_UPDATED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_HOLDING_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_HOLDING_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_HOLDING_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_ITEM_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_ITEM_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_ITEM_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_MARC_FOR_DELETE_RECEIVED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_MARC_FOR_UPDATE_RECEIVED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_NOT_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_HOLDINGS_RECORD_MATCHED;
import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE;

import java.util.Arrays;
import java.util.List;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope(SCOPE_PROTOTYPE)
public class DataImportConsumersVerticle extends AbstractConsumerVerticle<String, byte[]> {

  private static final List<String> EVENTS = Arrays.asList(
    DI_INVENTORY_AUTHORITY_CREATED_READY_FOR_POST_PROCESSING.value(),
    DI_INVENTORY_AUTHORITY_UPDATED_READY_FOR_POST_PROCESSING.value(),
    DI_INVENTORY_HOLDING_CREATED.value(),
    DI_INVENTORY_HOLDING_UPDATED.value(),
    DI_INVENTORY_HOLDING_MATCHED.value(),
    DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING.value(),
    DI_INVENTORY_INSTANCE_CREATED.value(),
    DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING.value(),
    DI_INVENTORY_INSTANCE_MATCHED.value(),
    DI_INVENTORY_INSTANCE_UPDATED.value(),
    DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING.value(),
    DI_INVENTORY_ITEM_CREATED.value(),
    DI_INVENTORY_ITEM_MATCHED.value(),
    DI_INVENTORY_ITEM_UPDATED.value(),
    DI_MARC_FOR_DELETE_RECEIVED.value(),
    DI_MARC_FOR_UPDATE_RECEIVED.value(),
    DI_SRS_MARC_AUTHORITY_RECORD_CREATED.value(),
    DI_SRS_MARC_AUTHORITY_RECORD_MATCHED.value(),
    DI_SRS_MARC_BIB_RECORD_CREATED.value(),
    DI_SRS_MARC_BIB_RECORD_MATCHED.value(),
    DI_SRS_MARC_BIB_RECORD_MODIFIED.value(),
    DI_SRS_MARC_BIB_RECORD_NOT_MATCHED.value(),
    DI_SRS_MARC_HOLDINGS_RECORD_MATCHED.value()
  );

  private final AsyncRecordHandler<String, byte[]> dataImportKafkaHandler;

  @Value("${srs.kafka.DataImportConsumer.loadLimit:5}")
  private int loadLimit;

  @Autowired
  public DataImportConsumersVerticle(KafkaConfig kafkaConfig,
                                     @Qualifier("DataImportKafkaHandler")
                                     AsyncRecordHandler<String, byte[]> dataImportKafkaHandler) {
    super(kafkaConfig);
    this.dataImportKafkaHandler = dataImportKafkaHandler;
  }

  @Override
  protected int loadLimit() {
    return loadLimit;
  }

  @Override
  protected AsyncRecordHandler<String, byte[]> recordHandler() {
    return dataImportKafkaHandler;
  }

  @Override
  protected List<String> eventTypes() {
    return EVENTS;
  }

  @Override
  public String getDeserializerClass() {
    return "org.apache.kafka.common.serialization.ByteArrayDeserializer";
  }

}
