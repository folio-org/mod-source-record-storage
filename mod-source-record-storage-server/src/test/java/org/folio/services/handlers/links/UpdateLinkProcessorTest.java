package org.folio.services.handlers.links;

import static org.junit.Assert.assertEquals;

import java.util.List;
import org.junit.Test;
import org.marc4j.marc.Subfield;
import org.marc4j.marc.impl.SubfieldImpl;

public class UpdateLinkProcessorTest {

  private final LinkProcessor processor = new UpdateLinkProcessor();

  @Test
  public void process_positive_updateSubfieldsAndOrderIt() {
    List<Subfield> oldSubfields = List.of(
      new SubfieldImpl('a', "a-data"),
      new SubfieldImpl('b', "b-data"),
      new SubfieldImpl('z', "z-data"),
      new SubfieldImpl('0', "0-data"),
      new SubfieldImpl('9', "9-data"),
      new SubfieldImpl('k', "k-data")
    );
    var subfieldsChanges = List.of(
      new org.folio.rest.jaxrs.model.Subfield().withCode("a").withValue("a-data"),
      new org.folio.rest.jaxrs.model.Subfield().withCode("0").withValue("0-data-new"),
      new org.folio.rest.jaxrs.model.Subfield().withCode("z").withValue("z-data-new1"),
      new org.folio.rest.jaxrs.model.Subfield().withCode("z").withValue("z-data-new2"),
      new org.folio.rest.jaxrs.model.Subfield().withCode("b").withValue("")
    );

    var actual = processor.process("100", subfieldsChanges, oldSubfields);

    var expected = List.of(
      oldSubfields.get(0),
      new SubfieldImpl('z', "z-data-new1"),
      new SubfieldImpl('z', "z-data-new2"),
      new SubfieldImpl('0', "0-data-new"),
      oldSubfields.get(4),
      oldSubfields.get(5)
    );

    assertEquals(expected.toString(), actual.toString());
  }

}
