package org.folio;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.State;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecordCollection;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;

public class SourceRecordTestHelper {

  public static List<Record> getRecords(State state) {
    return TestMocks.getRecords().stream()
      .filter(expectedRecord -> expectedRecord.getState().equals(state))
      .collect(Collectors.toList());
  }

  public static List<RawRecord> getRawRecords(List<Record> records) {
    return records.stream()
      .map(record -> TestMocks.getRawRecord(record.getId()))
      .filter(rawRecord -> rawRecord.isPresent())
      .map(rawRecord -> rawRecord.get())
      .collect(Collectors.toList());
  }

  public static List<ParsedRecord> getParsedRecords(List<Record> records) {
    return records.stream()
      .map(record -> TestMocks.getParsedRecord(record.getId()))
      .filter(parsedRecord -> parsedRecord.isPresent())
      .map(parsedRecord -> parsedRecord.get())
      .collect(Collectors.toList());
  }

  public static SourceRecord enhanceWithRawRecord(SourceRecord sourceRecord, List<RawRecord> rawRecords) {
    Optional<RawRecord> rawRecord = rawRecords.stream()
      .filter(rr -> rr.getId().equals(sourceRecord.getRecordId()))
      .findAny();
    if (rawRecord.isPresent()) {
      sourceRecord.withRawRecord(rawRecord.get());
    }
    return sourceRecord;
  }

  public static SourceRecord enhanceWithParsedRecord(SourceRecord sourceRecord, List<ParsedRecord> parsedRecords) {
    Optional<ParsedRecord> parsedRecord = parsedRecords.stream()
      .filter(pr -> pr.getId().equals(sourceRecord.getRecordId()))
      .findAny();
    if (parsedRecord.isPresent()) {
      sourceRecord.withParsedRecord(parsedRecord.get());
    }
    return sourceRecord;
  }

  public static void compareSourceRecord(
    TestContext context,
    Record expectedRecord,
    ParsedRecord expectedParsedRecord,
    Optional<SourceRecord> actualSourceRecord
  ) {
    context.assertTrue(actualSourceRecord.isPresent());
    context.assertEquals(expectedRecord.getId(), actualSourceRecord.get().getRecordId());
    context.assertEquals(new JsonObject((String) expectedParsedRecord.getContent()),
      new JsonObject((String) actualSourceRecord.get().getParsedRecord().getContent()));
    context.assertEquals(expectedParsedRecord.getFormattedContent().trim(),
      actualSourceRecord.get().getParsedRecord().getFormattedContent().trim());
  }

  public static void compareSourceRecordCollection(
    TestContext context, 
    List<Record> expectedRecords,
    List<RawRecord> expectedRawRecords,
    List<ParsedRecord> expectedParsedRecords, 
    SourceRecordCollection actualSourceRecordCollection
  ) {
    List<SourceRecord> actualSourceRecords = actualSourceRecordCollection.getSourceRecords();
    context.assertEquals(expectedRecords.size(), actualSourceRecordCollection.getTotalRecords());
    context.assertTrue(actualSourceRecordCollection.getTotalRecords() >= expectedRawRecords.size());
    context.assertTrue(actualSourceRecordCollection.getTotalRecords() >= expectedParsedRecords.size());
    compareSourceRecords(context, expectedRecords, expectedRawRecords, expectedParsedRecords, actualSourceRecords);
  }

  public static void compareSourceRecords(
    TestContext context, 
    List<Record> expectedRecords,
    List<RawRecord> expectedRawRecords,
    List<ParsedRecord> expectedParsedRecords, 
    List<SourceRecord> actualSourceRecords
  ) {

    context.assertEquals(expectedRecords.size(), actualSourceRecords.size());

    Collections.sort(expectedRecords, (r1, r2) -> r1.getId().compareTo(r2.getId()));
    Collections.sort(expectedRawRecords, (rr1, rr2) -> rr1.getId().compareTo(rr2.getId()));
    Collections.sort(expectedParsedRecords, (pr1, pr2) -> pr1.getId().compareTo(pr2.getId()));
    Collections.sort(actualSourceRecords, (sr1, sr2) -> sr1.getRecordId().compareTo(sr2.getRecordId()));

    expectedRawRecords.forEach(expectedRawRecord -> {
      Optional<SourceRecord> actualSourceRecord = actualSourceRecords.stream().filter(sourceRecord -> {
        return Objects.nonNull(sourceRecord.getRawRecord())
          && sourceRecord.getRawRecord().getId().equals(expectedRawRecord.getId());
      }).findAny();
      context.assertTrue(actualSourceRecord.isPresent());
      context.assertEquals(expectedRawRecord.getContent(),
        actualSourceRecord.get().getRawRecord().getContent());
    });

    expectedParsedRecords.forEach(expectedParsedRecord -> {
      Optional<SourceRecord> actualSourceRecord = actualSourceRecords.stream().filter(sourceRecord -> {
        return Objects.nonNull(sourceRecord.getParsedRecord())
          && sourceRecord.getParsedRecord().getId().equals(expectedParsedRecord.getId());
      }).findAny();
      context.assertTrue(actualSourceRecord.isPresent());
      context.assertEquals(new JsonObject((String) expectedParsedRecord.getContent()).encode(),
        new JsonObject((String) actualSourceRecord.get().getParsedRecord().getContent()).encode());
      context.assertEquals(expectedParsedRecord.getFormattedContent().trim(),
        actualSourceRecord.get().getParsedRecord().getFormattedContent().trim());
    });

    for (int i = 0; i < expectedRecords.size(); i++) {
      Record expectedRecord = expectedRecords.get(i);
      SourceRecord actualSourceRecord = actualSourceRecords.get(i);
      context.assertEquals(expectedRecord.getId(), actualSourceRecord.getRecordId());
      if (Objects.nonNull(actualSourceRecord.getSnapshotId())) {
        context.assertEquals(expectedRecord.getSnapshotId(), actualSourceRecord.getSnapshotId());
      }
      if (Objects.nonNull(actualSourceRecord.getOrder())) {
        context.assertEquals(expectedRecord.getOrder(), actualSourceRecord.getOrder());
      }
      if (Objects.nonNull(actualSourceRecord.getRecordType())) {
        context.assertEquals(expectedRecord.getRecordType().toString(),
          actualSourceRecord.getRecordType().toString());
      }
      if (Objects.nonNull(actualSourceRecord.getExternalIdsHolder())) {
        context.assertEquals(expectedRecord.getExternalIdsHolder().getInstanceId(),
          actualSourceRecord.getExternalIdsHolder().getInstanceId());
      }
      if (Objects.nonNull(actualSourceRecord.getMetadata())) {
        context.assertEquals(expectedRecord.getMetadata().getCreatedByUserId(),
          actualSourceRecord.getMetadata().getCreatedByUserId());
        // context.assertNotNull(actualSourceRecord.getMetadata().getCreatedDate());
        context.assertEquals(expectedRecord.getMetadata().getUpdatedByUserId(),
          actualSourceRecord.getMetadata().getUpdatedByUserId());
        // context.assertNotNull(actualSourceRecord.getMetadata().getUpdatedDate());
      }
    }
  }

}