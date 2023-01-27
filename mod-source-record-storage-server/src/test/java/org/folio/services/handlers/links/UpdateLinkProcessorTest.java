package org.folio.services.handlers.links;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.marc4j.marc.Subfield;
import org.marc4j.marc.impl.SubfieldImpl;

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

//  private class TestSubfield extends SubfieldImpl {
//
//    public TestSubfield(char code, String data) {
//      super(code, data);
//    }
//
//    @Override
//    public boolean equals(Object o) {
//      if (this == o) { return true; }
//
//      if (!(o instanceof TestSubfield)) { return false; }
//
//      TestSubfield that = (TestSubfield) o;
//
//      return new EqualsBuilder().append(getCode(), that.getCode())
//        .append(getData(), that.getData())
//        .isEquals();
//    }
//
//    @Override
//    public int hashCode() {
//      return new HashCodeBuilder(17, 37)
//        .append(getCode()).append(getData())
//        .toHashCode();
//    }
//  }
}
