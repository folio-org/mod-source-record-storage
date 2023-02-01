package org.folio.services.handlers.links;

import java.util.Collection;
import java.util.List;
import org.marc4j.marc.Subfield;

@FunctionalInterface
public interface LinkProcessor {

  Collection<Subfield> process(String fieldCode,
                               List<org.folio.rest.jaxrs.model.Subfield> subfieldsChanges,
                               List<Subfield> oldSubfields);
}
