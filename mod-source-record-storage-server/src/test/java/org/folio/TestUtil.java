package org.folio;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.folio.dbschema.ObjectMapperTool;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

public final class TestUtil {

  public static String readFileFromPath(String path) {
    try {
      return FileUtils.readFileToString(new File(path), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static String resourceToString(String resource) {
    try {
      return IOUtils.resourceToString(resource, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static <T> T clone(T obj, Class<T> type) {
    try {
      final ObjectMapper jsonMapper = ObjectMapperTool.getMapper();
      return jsonMapper.readValue(jsonMapper.writeValueAsString(obj), type);
    } catch (JsonProcessingException ex) {
      throw new IllegalArgumentException(ex);
    }
  }
}
