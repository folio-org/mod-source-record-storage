package org.folio.services.util;

import org.marc4j.MarcJsonReader;
import org.marc4j.marc.Record;
import org.marc4j.marc.impl.SortedRecordImpl;

import java.io.InputStream;

/**
 * Implementation of MarcReader, allows iterating over a collection of {@link SortedRecordImpl} objects
 */
public class SortedMarcJsonReader extends MarcJsonReader {

  public SortedMarcJsonReader(InputStream is) {
    super(is);
  }

  /**
   * Returns next record in the iteration with sorted fields by their tags
   *
   * @return record implementation with sorted fields {@link SortedRecordImpl}
   */
  public Record next() {
    Record record = super.next();
    Record sortedRecord = new SortedRecordImpl();
    sortedRecord.setLeader(record.getLeader());
    record.getVariableFields().forEach(sortedRecord::addVariableField);
    return sortedRecord;
  }
}
