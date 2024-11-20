package org.folio.services.handlers.links;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.folio.util.AuthorityLinksUtils.AUTHORITY_ID_SUBFIELD;
import static org.folio.util.AuthorityLinksUtils.AUTHORITY_NATURAL_ID_SUBFIELD;
import static org.folio.util.AuthorityLinksUtils.getAuthorityIdSubfield;
import static org.folio.util.AuthorityLinksUtils.getAuthorityNaturalIdSubfield;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.marc4j.marc.Subfield;
import org.marc4j.marc.impl.SubfieldImpl;

public class UpdateLinkProcessor implements LinkProcessor {

  @Override
  public Collection<Subfield> process(String fieldCode, List<org.folio.rest.jaxrs.model.Subfield> subfieldsChanges,
                                      List<Subfield> oldSubfields) {
    var authorityIdSubfield = getAuthorityIdSubfield(oldSubfields);
    var authorityNaturalIdSubfield = getAuthorityNaturalIdSubfield(oldSubfields);
    var controllableSubfields = new ArrayList<Subfield>();
    var controllableSubfieldCodes = initializeSubfieldCodes();

    for (var subfieldsChange : subfieldsChanges) {
      var code = subfieldsChange.getCode().charAt(0);
      var value = subfieldsChange.getValue();
      switch (code) {
        case AUTHORITY_ID_SUBFIELD -> authorityIdSubfield = Optional.of(new SubfieldImpl(code, value));
        case AUTHORITY_NATURAL_ID_SUBFIELD -> authorityNaturalIdSubfield = Optional.of(new SubfieldImpl(code, value));
        default -> {
          if (isNotBlank(value)) {
            controllableSubfields.add(new SubfieldImpl(code, value));
          }
        }
      }
      controllableSubfieldCodes.add(code);
    }

    // add controllable subfields
    var result = new ArrayList<>(controllableSubfields);

    // add special subfields
    authorityNaturalIdSubfield.ifPresent(result::add);
    authorityIdSubfield.ifPresent(result::add);

    // add uncontrollable subfields
    for (var oldSubfield : oldSubfields) {
      if (isNotControllableSubfield(oldSubfield, controllableSubfieldCodes)) {
        result.add(oldSubfield);
      }
    }

    return result;
  }

  private boolean isNotControllableSubfield(Subfield subfield, Set<Character> controllableSubfieldCodes) {
    return !controllableSubfieldCodes.contains(subfield.getCode());
  }

  private Set<Character> initializeSubfieldCodes() {
    return new HashSet<>(Arrays.asList(AUTHORITY_ID_SUBFIELD, AUTHORITY_NATURAL_ID_SUBFIELD));
  }
}
