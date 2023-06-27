package org.folio.dao.util;

import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.folio.TestUtil;
import org.jooq.DSLContext;
import org.jooq.Record7;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.FileReader;
import java.io.Reader;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.apache.commons.csv.CSVFormat.EXCEL;
import static org.folio.dao.util.ParsedRecordDaoUtil.MARC_INDEXERS_TABLE;
import static org.junit.Assert.assertEquals;

@RunWith(VertxUnitRunner.class)
public class ParsedRecordDaoUtilTest {

  private static final String PARSED_MARC_RECORD_SAMPLE_PATH =
    "src/test/resources/org/folio/dao/util/parsedMarcRecord.json";
  private static final String PARSED_MARC_RECORD_BAD_SAMPLE_PATH =
    "src/test/resources/org/folio/dao/util/parsedMarcRecordBad.json";
  private static final String MARC_INDEXER_SAMPLE_PATH =
    "src/test/resources/org/folio/dao/util/marc_indexer_row.csv";
  private static final DSLContext DSL_CONTEXT = DSL.using(SQLDialect.POSTGRES);
  private static final UUID MARC_ID = UUID.fromString("9a4db741-2acb-4ad8-9e66-ab6d17dcbe68");
  private static final Integer VERSION = 1;

  /**
   * A single record import was performed with SRS prior to MODSOURCE-664. The parsed record in MARC_RECORDS_LB and the
   * MARC_INDEXERS row are saved in this test suite for comparison to the marc indexer generator developed as a result
   * of MODSOURCE-664 . A small difference in the MARC_INDEXERS rows saved in this test suite
   * is that the MARC_INDEXERS rows have the 010 field's value trimmed. The new marc indexer generator trims its values
   * while the SQL version did not.
   */
  @Test
  public void createMarcIndexerRecord() throws Exception {
    String content = TestUtil.readFileFromPath(PARSED_MARC_RECORD_SAMPLE_PATH);
    Set<Record7<String, String, String, String, String, UUID, Integer>> expected = parseCSV(MARC_INDEXER_SAMPLE_PATH);

    Set<org.jooq.Record> records =
      ParsedRecordDaoUtil.createMarcIndexerRecord(DSL_CONTEXT, MARC_ID, content, VERSION);

    assertEquals(expected, records);
  }

  @Test(expected = IllegalArgumentException.class)
  public void badParsedRecord() throws Exception {
    String content = TestUtil.readFileFromPath(PARSED_MARC_RECORD_BAD_SAMPLE_PATH);

    ParsedRecordDaoUtil.createMarcIndexerRecord(DSL_CONTEXT, MARC_ID, content, VERSION);
  }

  @Test(expected = IllegalArgumentException.class)
  public void notJsonContent() throws Exception {
    String content = "This is a not a parsed record";

    ParsedRecordDaoUtil.createMarcIndexerRecord(DSL_CONTEXT, MARC_ID, content, VERSION);
  }

  private Set<Record7<String, String, String, String, String, UUID, Integer>> parseCSV(String filePath) throws Exception {
    Set<Record7<String, String, String, String, String, UUID, Integer>> records = new HashSet<>();

    try (Reader in = new FileReader(filePath)) {
      Iterable<CSVRecord> csvRecords = CSVFormat.Builder.create(EXCEL)
        .setHeader()
        .setSkipHeaderRecord(true)
        .build()
        .parse(in);

      for (CSVRecord csvRecord : csvRecords) {
        Record7<String, String, String, String, String, UUID, Integer> record = createRecord(
          csvRecord.get(0),
          csvRecord.get(1),
          csvRecord.get(2),
          csvRecord.get(3),
          csvRecord.get(4),
          UUID.fromString(csvRecord.get(5)),
          Integer.parseInt(csvRecord.get(6))
        );
        records.add(record);
      }
    }

    return records;
  }

  private Record7<String, String, String, String, String, UUID, Integer> createRecord(
    String col1,
    String col2,
    String col3,
    String col4,
    String col5,
    UUID col6,
    Integer col7
  ) {
    return DSL_CONTEXT.newRecord(
        MARC_INDEXERS_TABLE.FIELD_NO,
        MARC_INDEXERS_TABLE.IND1,
        MARC_INDEXERS_TABLE.IND2,
        MARC_INDEXERS_TABLE.SUBFIELD_NO,
        MARC_INDEXERS_TABLE.VALUE,
        MARC_INDEXERS_TABLE.MARC_ID,
        MARC_INDEXERS_TABLE.VERSION)
      .values(col1, col2, col3, col4, col5, col6, col7);
  }
}
