package org.folio.services.handlers.links;

import static org.folio.util.AuthorityLinksUtils.AUTHORITY_ID_SUBFIELD;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import org.marc4j.marc.Subfield;

public class DeleteLinkProcessor implements LinkProcessor {

  @Override
  public Collection<Subfield> process(String fieldCode, List<org.folio.rest.jaxrs.model.Subfield> subfieldsChanges,
                                      List<Subfield> oldSubfields) {
    var newSubfields = new LinkedList<>(oldSubfields);

    newSubfields.removeIf(subfield -> subfield.getCode() == AUTHORITY_ID_SUBFIELD);

    return newSubfields;
  }
}
