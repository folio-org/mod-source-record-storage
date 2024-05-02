package org.folio.services;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
  AdditionalFieldsUtilTest.class,
  AdditionalFieldsUtilTest.OclcFieldNormalizationTest.class
})
public class AdditionalFieldsUtilTestSuite {
}
