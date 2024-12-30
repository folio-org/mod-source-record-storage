package org.folio.util;

import java.util.List;
import java.util.Optional;
import lombok.experimental.UtilityClass;
import org.marc4j.marc.Subfield;

@UtilityClass
public class AuthorityLinksUtils {

  public static final char AUTHORITY_ID_SUBFIELD = '9';
  public static final char AUTHORITY_NATURAL_ID_SUBFIELD = '0';

  public static Optional<Subfield> getAuthorityIdSubfield(List<Subfield> subfields) {
    return getSubfield(subfields, AUTHORITY_ID_SUBFIELD);
  }

  public static Optional<Subfield> getAuthorityNaturalIdSubfield(List<Subfield> subfields) {
    return getSubfield(subfields, AUTHORITY_NATURAL_ID_SUBFIELD);
  }

  private static Optional<Subfield> getSubfield(List<Subfield> subfields, char authorityIdSubfield) {
    return subfields.stream()
      .filter(subfield -> subfield.getCode() == authorityIdSubfield)
      .findFirst();
  }
}
