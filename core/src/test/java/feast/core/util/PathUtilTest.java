package feast.core.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class PathUtilTest {

  @Test
  public void testGetPath() {
    assertEquals("file:///tmp/foo/bar", PathUtil.getPath("/tmp/foo/bar").toUri().toString());
  }

  @Test
  public void testGetPath_withGcs() {

    assertEquals("gs://tmp/foo/bar", PathUtil.getPath("gs://tmp/foo/bar").toUri().toString());
  }
}