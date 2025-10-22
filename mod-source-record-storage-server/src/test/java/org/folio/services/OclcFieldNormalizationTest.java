package org.folio.services;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.util.AdditionalFieldsUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class OclcFieldNormalizationTest {

  @Parameterized.Parameter(0)
  public String parsedContent;

  @Parameterized.Parameter(1)
  public String expectedParsedContent;

  @Parameterized.Parameters(name = "{index}: parsedContent={0}, expectedParsedContent={1}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
      {
        "{\"leader\":\"00120nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(ybp7406411)in001\"}," +
          "{\"a\":\"(OCoLC)00006475800\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}",

        "{\"leader\":\"00115nam  22000611a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(ybp7406411)in001\"}," +
          "{\"a\":\"(OCoLC)6475800\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}"
      },
      {
        "{\"leader\":\"00120nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(ybp7406411)in001\"}," +
          "{\"a\":\"(OCoLC)tfe0006475800\"} ],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}",

        "{\"leader\":\"00118nam  22000611a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(ybp7406411)in001\"}," +
          "{\"a\":\"(OCoLC)tfe6475800\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}"
      },
      {
        "{\"leader\":\"00120nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(ybp7406411)in001\"}," +
          "{\"a\":\"(OCoLC)00064758\"}," +
          "{\"a\":\"(OCoLC)ocm00064758\"}," +
          "{\"z\":\"(OCoLC)00024758\"} ],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}",

        "{\"leader\":\"00127nam  22000611a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(ybp7406411)in001\"}," +
          "{\"a\":\"(OCoLC)64758\"},{\"z\":\"(OCoLC)24758\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}"
      },
      {
        "{\"leader\":\"00120nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)00064758\"} ],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)ocn000064758\"} ],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)ocm0000064758\"}, {\"z\":\"(OCoLC)11114758\"} ],\"ind1\":\" \"," +
          "\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}",

        "{\"leader\":\"00111nam  22000611a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)64758\"},{\"z\":\"(OCoLC)11114758\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}"
      },
      {
        "{\"leader\":\"00120nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(ybp7406411)in001\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)ocn00064758\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)ocm000064758\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}",

        "{\"leader\":\"00128nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(ybp7406411)in001\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)64758\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}"
      },
      {
        "{\"leader\":\"00120nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)ocn607TST001\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}",

        "{\"leader\":\"00098nam  22000611a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)607TST001\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}"
      },
      {
        "{\"leader\":\"00120nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC-M)ocn0001234\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(ABC)ocn0001234\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)ocn0001234\"}, {\"a\":\"(OCoLC)ocn1234\"}, {\"b\":\"(OCoLC)ocn1234\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)ocm1234\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)ocn00098765\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)ocn0001234\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}",

        "{\"leader\":\"00218nam  22001091a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC-M)ocn0001234\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(ABC)ocn0001234\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"b\":\"(OCoLC)1234\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)98765\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)1234\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}"
      },
      {
        "{\"leader\":\"00126nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)1234\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC-M)1234456\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}",

        "{\"leader\":\"00126nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)1234\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC-M)1234456\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}"
      },
      {
        "{\"leader\":\"00126nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(amdb)4450080\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)56072293\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"z\":\"(OCoLC)50824229\"}, {\"z\":\"(OCoLC)81729337\"}, {\"z\":\"(OCoLC)776983186\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}",

        "{\"leader\":\"00194nam  22000851a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(amdb)4450080\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)56072293\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"z\":\"(OCoLC)50824229\"},{\"z\":\"(OCoLC)81729337\"},{\"z\":\"(OCoLC)776983186\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}"
      }
    });
  }

  @Test
  public void shouldNormalizeOCoLCField035() {
    // given
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedContent);

    Record record = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));
    // when
    AdditionalFieldsUtil.normalize035(record);
    Assert.assertEquals(expectedParsedContent, parsedRecord.getContent());
  }
}
