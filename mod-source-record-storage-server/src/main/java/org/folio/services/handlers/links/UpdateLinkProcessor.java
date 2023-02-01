package org.folio.services.handlers.links;

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.Collection;
import java.util.List;
import java.util.TreeMap;
import org.marc4j.marc.Subfield;
import org.marc4j.marc.impl.SubfieldImpl;

public class UpdateLinkProcessor implements LinkProcessor {

  @Override
  public Collection<Subfield> process(String fieldCode, List<org.folio.rest.jaxrs.model.Subfield> subfieldsChanges,
                                      List<Subfield> oldSubfields) {
    var newSubfields = new TreeMap<Character, Subfield>((x, y) -> {
      var a = Character.isDigit(x) ? x + 100 : x;
      var b = Character.isDigit(y) ? y + 100 : y;
      return a - b;
    });

    //add old subfields
    oldSubfields.forEach(subfield -> newSubfields.put(subfield.getCode(), subfield));

    //add subfields from incoming event, removing ones with empty value
    subfieldsChanges.forEach(subfield -> {
      var code = subfield.getCode().charAt(0);
      if (isBlank(subfield.getValue())) {
        newSubfields.remove(code);
        return;
      }
      newSubfields.put(code, new SubfieldImpl(code, subfield.getValue()));
    });

    return newSubfields.values();
  }
}
