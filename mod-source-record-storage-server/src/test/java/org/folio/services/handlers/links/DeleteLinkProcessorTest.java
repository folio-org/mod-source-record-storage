package org.folio.services.handlers.links;

import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collections;
import java.util.List;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.marc4j.marc.impl.SubfieldImpl;

public class DeleteLinkProcessorTest {

  private final LinkProcessor processor = new DeleteLinkProcessor();

  @Test
  public void process_positive_delete9Subfield() {
    List<org.marc4j.marc.Subfield> oldSubfields = List.of(
      new SubfieldImpl('a', "a-data"),
      new SubfieldImpl('z', "z-data"),
      new SubfieldImpl('0', "0-data"),
      new SubfieldImpl('9', "9-data")
    );
    var actual = processor.process("100", Collections.emptyList(), oldSubfields);

    assertThat(actual, Matchers.contains(oldSubfields.get(0), oldSubfields.get(1), oldSubfields.get(2)));
  }

  @Test
  public void process_positive_doNothingWhen9SubfieldIsNotPresent() {
    List<org.marc4j.marc.Subfield> oldSubfields = List.of(
      new SubfieldImpl('a', "a-data"),
      new SubfieldImpl('z', "z-data"),
      new SubfieldImpl('0', "0-data")
    );
    var actual = processor.process("100", Collections.emptyList(), oldSubfields);

    assertThat(actual, Matchers.contains(oldSubfields.get(0), oldSubfields.get(1), oldSubfields.get(2)));
  }
}
