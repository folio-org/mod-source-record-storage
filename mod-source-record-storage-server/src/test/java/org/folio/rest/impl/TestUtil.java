package org.folio.rest.impl;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public final class TestUtil {

  public static String readFileFromPath(String path) throws IOException {
    return new String(FileUtils.readFileToByteArray(new File(path)));
  }
}
