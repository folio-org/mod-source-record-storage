package org.folio.services.entities;

import org.folio.rest.jaxrs.model.RecordCollection;
import java.util.Objects;
import java.util.function.UnaryOperator;

@FunctionalInterface
public
interface RecordsModifierOperator extends UnaryOperator<RecordCollection> {

  static RecordsModifierOperator identity() {
    return s -> s;
  }

  default RecordsModifierOperator andThen(RecordsModifierOperator after) {
    Objects.requireNonNull(after);
    return s -> after.apply(this.apply(s));
  }

  default RecordsModifierOperator compose(RecordsModifierOperator before) {
    Objects.requireNonNull(before);
    return s -> this.apply(before.apply(s));
  }
}
