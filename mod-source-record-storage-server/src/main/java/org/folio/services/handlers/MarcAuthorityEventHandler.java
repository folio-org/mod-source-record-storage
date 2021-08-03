package org.folio.services.handlers;

import static org.folio.rest.jaxrs.model.EntityType.MARC_AUTHORITY;

import org.springframework.stereotype.Component;

@Component
public class MarcAuthorityEventHandler extends AbstractMarcEventHandler {

  @Override
  public String getEntityType() {
    return MARC_AUTHORITY.value();
  }
}
