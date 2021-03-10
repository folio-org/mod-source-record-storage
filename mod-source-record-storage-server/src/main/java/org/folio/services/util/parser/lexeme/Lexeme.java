package org.folio.services.util.parser.lexeme;

public interface Lexeme {

  LexemeType getType();

  String toSqlRepresentation();
}
