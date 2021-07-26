package org.folio.services.handlers;

import static org.folio.rest.jaxrs.model.EntityType.MARC_HOLDINGS;

import org.springframework.stereotype.Component;

@Component
public class MarcHoldingsEventHandler extends AbstractMarcEventHandler {

  @Override
  public String getEntityType() {
    return MARC_HOLDINGS.value();
  }
}
