package org.folio.services;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;

import java.util.UUID;
import org.folio.processing.util.MarcRecordNormalizer;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.util.AdditionalFieldsUtil;
import org.junit.Test;

/**
 * Tests that {@link AdditionalFieldsUtil#normalize035} correctly delegates to
 * {@link MarcRecordNormalizer#normalize035Field} and then recalculates the FOLIO
 * record's leader.  The normalization logic itself is covered in MarcRecordNormalizerTest
 * (data-import-processing-core).
 */
public class OclcFieldNormalizationTest {

  private static final String CONTENT_WITH_035 =
    "{\"leader\":\"00000nam  22000000a 4500\",\"fields\":["
      + "{\"001\":\"in001\"},"
      + "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)64758\"}],\"ind1\":\" \",\"ind2\":\" \"}}"
      + "]}";

  @Test
  public void normalize035_delegatesToNormalizerAndRecalculatesLeader() {
    var parsedRecord = new ParsedRecord().withContent(CONTENT_WITH_035);
    var record = buildRecord(parsedRecord);

    try (var mocked = mockStatic(MarcRecordNormalizer.class)) {
      AdditionalFieldsUtil.normalize035(record);

      // normalizer was invoked with the resolved marc4j record
      mocked.verify(() -> MarcRecordNormalizer.normalize035Field(any(org.marc4j.marc.Record.class)));

      // leader was recalculated: content string differs from the placeholder input
      var updatedContent = (String) parsedRecord.getContent();
      assertNotNull(updatedContent);
      assertNotEquals(CONTENT_WITH_035, updatedContent);
    }
  }

  @Test
  public void normalize035_noParsedRecord_normalizerNotCalled() {
    var marcRecord = buildRecord(null);

    try (var mocked = mockStatic(MarcRecordNormalizer.class)) {
      AdditionalFieldsUtil.normalize035(marcRecord);

      mocked.verify(() -> MarcRecordNormalizer.normalize035Field(any()), never());
    }
  }

  private static Record buildRecord(ParsedRecord parsedRecord) {
    return new Record()
      .withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));
  }
}
