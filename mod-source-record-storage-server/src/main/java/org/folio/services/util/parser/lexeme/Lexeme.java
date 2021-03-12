package org.folio.services.util.parser.lexeme;

/**
 * The root interface for all the lexemes that are available for the incoming search expression.
 * @see LexemeType
 * @see Lexicon
 */
public interface Lexeme {

  LexemeType getType();

  String toSqlRepresentation();
}
