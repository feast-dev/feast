package feast.ingestion.util;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import feast.specs.ImportJobSpecsProto.ImportJobSpecs;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Test;

public class ProtoUtilTest {

  @Test
  public void createProtoMessageFromYaml_valid1() throws IOException, URISyntaxException {
    String testYamlFilePath =
        new File("src/test/resources/import-job-specs/valid-1.yaml").getAbsolutePath();
    Message message =
        ProtoUtil.createProtoMessageFromYamlFileUri(
            testYamlFilePath, ImportJobSpecs.newBuilder(), ImportJobSpecs.class);
    assert message != null;
  }

  @Test
  public void createProtoMessageFromYaml_valid2() throws IOException, URISyntaxException {
    String testYamlFilePath =
        new File("src/test/resources/import-job-specs/valid-2.yaml").getAbsolutePath();
    Message message =
        ProtoUtil.createProtoMessageFromYamlFileUri(
            testYamlFilePath, ImportJobSpecs.newBuilder(), ImportJobSpecs.class);
    assert message != null;
  }

  @Test(expected = MismatchedInputException.class)
  public void createProtoMessageFromYaml_invalidEmpty() throws IOException, URISyntaxException {
    String testYamlFilePath =
        new File("src/test/resources/import-job-specs/invalid-empty.yaml").getAbsolutePath();
    ProtoUtil.createProtoMessageFromYamlFileUri(
        testYamlFilePath, ImportJobSpecs.newBuilder(), ImportJobSpecs.class);
  }

  @Test(expected = InvalidProtocolBufferException.class)
  public void createProtoMessageFromYaml_invalidSourceSpec1()
      throws IOException, URISyntaxException {
    String testYamlFilePath =
        new File("src/test/resources/import-job-specs/invalid-source-spec-1.yaml")
            .getAbsolutePath();
    ProtoUtil.createProtoMessageFromYamlFileUri(
        testYamlFilePath, ImportJobSpecs.newBuilder(), ImportJobSpecs.class);
  }

  @Test(expected = IOException.class)
  public void createProtoMessageFromYaml_invalidFilePath() throws IOException, URISyntaxException {
    String testYamlFilePath = new File("this-path-should-not-exist").getAbsolutePath();
    ProtoUtil.createProtoMessageFromYamlFileUri(
        testYamlFilePath, ImportJobSpecs.newBuilder(), ImportJobSpecs.class);
  }

  @Test
  public void readStringFromUri_fileScheme_valid() throws IOException, URISyntaxException {
    Path tempFile = Files.createTempFile(null, null);
    Files.write(tempFile, "Dummy content".getBytes());
    String input = "file://" + tempFile.toAbsolutePath();
    String expected = "Dummy content";
    String actual = ProtoUtil.readStringFromUri(input);
    assertEquals(expected, actual);
  }

  @Test(expected = IllegalArgumentException.class)
  public void readStringFromUri_fileScheme_invalid_dueToNotProvidingAbsolutePath()
      throws IOException, URISyntaxException {
    String input = "file://relative/path";
    ProtoUtil.readStringFromUri(input);
  }

  @Test(expected = IllegalArgumentException.class)
  public void readStringFromUri_fileScheme_invalid_dueToProvidingDirectoryPath()
      throws IOException, URISyntaxException {
    String input = "file:///absolute/path/to/directory/";
    ProtoUtil.readStringFromUri(input);
  }

  @Test
  public void readStringFromUri_gsScheme_valid() throws IOException, URISyntaxException {
    // TODO
    // Mock storage, which returns actual content when calling blob.getContent
  }

  // TODO: test for invalid arguments to readStringFromUri
}
