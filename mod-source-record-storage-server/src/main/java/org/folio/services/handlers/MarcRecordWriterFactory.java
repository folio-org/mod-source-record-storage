package org.folio.services.handlers;

import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.mapping.mapper.writer.WriterFactory;
import org.folio.processing.mapping.mapper.writer.marc.MarcRecordWriter;
import org.folio.rest.jaxrs.model.EntityType;

public class MarcRecordWriterFactory implements WriterFactory {
  @Override
  public Writer createWriter() {
    return new MarcRecordWriter(EntityType.MARC_BIBLIOGRAPHIC);
  }

  @Override
  public boolean isEligibleForEntityType(EntityType entityType) {
    return EntityType.MARC_BIBLIOGRAPHIC == entityType;
  }
}
