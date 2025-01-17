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
    if (isOnlyNaturalIdChanged(subfieldsChanges)) {
      return processOnlyNaturalIdChange(oldSubfields, subfieldsChanges.get(0).getValue());
    }

    var authorityIdSubfield = getAuthorityIdSubfield(oldSubfields);
    var authorityNaturalIdSubfield = getAuthorityNaturalIdSubfield(oldSubfields);
    var controllableAlphaSubfields = new ArrayList<Subfield>();
    var controllableDigitSubfields = new ArrayList<Subfield>();
    var controllableSubfieldCodes = initializeSubfieldCodes();

    for (var subfieldsChange : subfieldsChanges) {
      var code = subfieldsChange.getCode().charAt(0);
      var value = subfieldsChange.getValue();
      switch (code) {
        case AUTHORITY_ID_SUBFIELD -> authorityIdSubfield = Optional.of(new SubfieldImpl(code, value));
        case AUTHORITY_NATURAL_ID_SUBFIELD -> authorityNaturalIdSubfield = Optional.of(new SubfieldImpl(code, value));
        default -> {
          if (isNotBlank(value)) {
            if (Character.isAlphabetic(code)) {
              controllableAlphaSubfields.add(new SubfieldImpl(code, value));
            } else {
              controllableDigitSubfields.add(new SubfieldImpl(code, value));
            }
          }
        }
      }
      controllableSubfieldCodes.add(code);
    }

    // add controllable alphabetic subfields
    var result = new ArrayList<>(controllableAlphaSubfields);

    // add uncontrollable alphabetic subfields
    var uncontrolledSubfields = oldSubfields.stream()
      .filter(subfield -> isNotControllableSubfield(subfield, controllableSubfieldCodes))
      .toList();
    uncontrolledSubfields.forEach(subfield -> {
      if (Character.isAlphabetic(subfield.getCode())) {
        result.add(subfield);
      }
    });

    //add special/digit controlled subfields
    authorityNaturalIdSubfield.ifPresent(result::add);
    authorityIdSubfield.ifPresent(result::add);
    result.addAll(controllableDigitSubfields);

    // add uncontrollable digit subfields
    uncontrolledSubfields.forEach(subfield -> {
      if (!Character.isAlphabetic(subfield.getCode())) {
        result.add(subfield);
      }
    });

    return result;
  }

  private Collection<Subfield> processOnlyNaturalIdChange(List<Subfield> oldSubfields, String newNaturalIdSubfield) {
    oldSubfields.stream()
      .filter(subfield -> subfield.getCode() == AUTHORITY_NATURAL_ID_SUBFIELD)
      .findFirst()
      .ifPresent(subfield -> subfield.setData(newNaturalIdSubfield));
    return oldSubfields;
  }

  private boolean isOnlyNaturalIdChanged(List<org.folio.rest.jaxrs.model.Subfield> subfieldsChanges) {
    return subfieldsChanges.size() == 1
      && subfieldsChanges.get(0).getCode().charAt(0) == AUTHORITY_NATURAL_ID_SUBFIELD;
  }

  private boolean isNotControllableSubfield(Subfield subfield, Set<Character> controllableSubfieldCodes) {
    return !controllableSubfieldCodes.contains(subfield.getCode());
  }

  private Set<Character> initializeSubfieldCodes() {
    return new HashSet<>(Arrays.asList(AUTHORITY_ID_SUBFIELD, AUTHORITY_NATURAL_ID_SUBFIELD));
  }
}
