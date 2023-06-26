package org.folio.services.handlers.links;


import java.util.List;

import org.junit.Test;
import org.marc4j.marc.Subfield;
import org.marc4j.marc.impl.SubfieldImpl;

import static org.junit.Assert.assertEquals;


class UpdateLinkProcessorTest {

  private final LinkProcessor processor = new UpdateLinkProcessor();

  @Test
  void process_positive_updateSubfieldsAndOrderIt() {
    List<Subfield> oldSubfields = List.of(
      new SubfieldImpl('a', "a-data"),
      new SubfieldImpl('z', "z-data"),
      new SubfieldImpl('0', "0-data"),
      new SubfieldImpl('9', "9-data")
    );
    var subfieldsChanges = List.of(
      new org.folio.rest.jaxrs.model.Subfield().withCode("0").withValue("0-data-new"),
      new org.folio.rest.jaxrs.model.Subfield().withCode("z").withValue("z-data-new")
    );

    var actual = processor.process("100", subfieldsChanges, oldSubfields);

    var expected = List.of(
      oldSubfields.get(0),
      new SubfieldImpl('z', "z-data-new"),
      new SubfieldImpl('0', "0-data-new"),
      oldSubfields.get(3)
    );

    assertEquals(actual.toString(), expected.toString());
  }

}
