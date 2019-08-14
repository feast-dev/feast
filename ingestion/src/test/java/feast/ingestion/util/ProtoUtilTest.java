package feast.ingestion.util;

import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import feast.specs.ImportJobSpecsProto.ImportJobSpecs;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class ProtoUtilTest {

  @Test
  public void createProtoMessageFromYaml_valid1() throws IOException {
    String testYamlFilePath =
        new File("src/test/resources/import-job-specs/valid-1.yaml").getAbsolutePath();
    Message message =
        ProtoUtil.createProtoMessageFromYaml(testYamlFilePath, ImportJobSpecs.newBuilder());
    assert message instanceof ImportJobSpecs;
  }

  @Test
  public void createProtoMessageFromYaml_valid2() throws IOException {
    String testYamlFilePath =
            new File("src/test/resources/import-job-specs/valid-2.yaml").getAbsolutePath();
    Message message =
            ProtoUtil.createProtoMessageFromYaml(testYamlFilePath, ImportJobSpecs.newBuilder());
    assert message instanceof ImportJobSpecs;
  }

  @Test(expected = MismatchedInputException.class)
  public void createProtoMessageFromYaml_invalidEmpty() throws IOException {
    String testYamlFilePath =
        new File("src/test/resources/import-job-specs/invalid-empty.yaml").getAbsolutePath();
    ProtoUtil.createProtoMessageFromYaml(testYamlFilePath, ImportJobSpecs.newBuilder());
  }

  @Test(expected = InvalidProtocolBufferException.class)
  public void createProtoMessageFromYaml_invalidSourceSpec1() throws IOException {
    String testYamlFilePath =
        new File("src/test/resources/import-job-specs/invalid-source-spec-1.yaml")
            .getAbsolutePath();
    ProtoUtil.createProtoMessageFromYaml(testYamlFilePath, ImportJobSpecs.newBuilder());
  }

  @Test(expected = IOException.class)
  public void createProtoMessageFromYaml_invalidFilePath() throws IOException {
    String testYamlFilePath = new File("this-path-should-not-exist").getAbsolutePath();
    ProtoUtil.createProtoMessageFromYaml(testYamlFilePath, ImportJobSpecs.newBuilder());
  }
}
