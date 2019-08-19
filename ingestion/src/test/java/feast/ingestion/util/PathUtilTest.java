package feast.ingestion.util;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Test;
import org.mockito.Mockito;

public class PathUtilTest {

  @Test
  public void readStringFromUri_fileScheme_valid() throws IOException, URISyntaxException {
    Path tempFile = Files.createTempFile(null, null);
    Files.write(tempFile, "Dummy content".getBytes());
    String input = "file://" + tempFile.toAbsolutePath();
    String expected = "Dummy content";
    String actual = PathUtil.readStringFromUri(input);
    assertEquals(expected, actual);
  }

  @Test(expected = IllegalArgumentException.class)
  public void readStringFromUri_fileScheme_invalid_dueToNotProvidingAbsolutePath()
      throws IOException, URISyntaxException {
    String input = "file://relative/path";
    PathUtil.readStringFromUri(input);
  }

  @Test(expected = IllegalArgumentException.class)
  public void readStringFromUri_fileScheme_invalid_dueToProvidingDirectoryPath()
      throws IOException, URISyntaxException {
    String input = "file:///absolute/path/to/directory/";
    PathUtil.readStringFromUri(input);
  }

  @Test
  public void readStringFromUri_gsScheme_valid() throws IOException, URISyntaxException {
    String input = "gs://bucket/path/to/blob";
    String expected = "blob content line 1\nblob content line 2";

    Blob mockBlob = Mockito.mock(Blob.class);
    when(mockBlob.getContent()).thenReturn(expected.getBytes(StandardCharsets.UTF_8));
    Storage mockStorage = Mockito.mock(Storage.class);
    when(mockStorage.get("bucket", "path/to/blob")).thenReturn(mockBlob);

    String actual = PathUtil.readStringFromUri(input, mockStorage);
    assertEquals(expected, actual);
  }

  @Test(expected = IllegalArgumentException.class)
  public void readStringFromUri_gsScheme_invalid_dueToEmptyContent()
      throws IOException, URISyntaxException {
    String input = "gs://bucket/path/to/blob";

    Blob mockBlob = Mockito.mock(Blob.class);
    when(mockBlob.getContent()).thenReturn("".getBytes(StandardCharsets.UTF_8));
    Storage mockStorage = Mockito.mock(Storage.class);
    when(mockStorage.get("bucket", "path/to/blob")).thenReturn(mockBlob);

    PathUtil.readStringFromUri(input, mockStorage);
  }

  @Test(expected = URISyntaxException.class)
  public void readStringFromUri_gsScheme_invalid_dueToMissingBucketName()
      throws IOException, URISyntaxException {
    String input = "gs://";
    Storage mockStorage = Mockito.mock(Storage.class);
    PathUtil.readStringFromUri(input, mockStorage);
  }

  @Test(expected = IllegalArgumentException.class)
  public void readStringFromUri_gsScheme_invalid_dueToMissingBlobName()
      throws IOException, URISyntaxException {
    String input = "gs://bucket";
    Storage mockStorage = Mockito.mock(Storage.class);
    PathUtil.readStringFromUri(input, mockStorage);
  }

  @Test(expected = IllegalArgumentException.class)
  public void readStringFromUri_gsScheme_invalid_dueToInvalidBlobName()
      throws IOException, URISyntaxException {
    String input = "gs://bucket/invalid/blob/that/ends/with/slash/";
    Storage mockStorage = Mockito.mock(Storage.class);
    PathUtil.readStringFromUri(input, mockStorage);
  }
}
