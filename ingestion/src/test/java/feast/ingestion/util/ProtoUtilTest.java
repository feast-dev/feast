//package feast.ingestion.util;
//
//import static org.junit.Assert.assertEquals;
//
//import feast.DriverAreaProto.DriverArea;
//import java.io.BufferedWriter;
//import java.io.IOException;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import lombok.extern.slf4j.Slf4j;
//import org.junit.Ignore;
//import org.junit.Rule;
//import org.junit.Test;
//import org.junit.rules.TemporaryFolder;
//
//@Slf4j
//public class ProtoUtilTest {
//
//  @Rule
//  public TemporaryFolder tempFolder = new TemporaryFolder();
//
//  @Test
//  public void testDecodeProtoYaml() throws IOException {
//
//    String yaml = ""
//        + "driverId: 1\n"
//        + "areaId: 2";
//
//    DriverArea da = ProtoUtil.decodeProtoYaml(yaml, DriverArea.getDefaultInstance());
//    assertEquals(1, da.getDriverId());
//    assertEquals(2, da.getAreaId());
//  }
//
//  @Test
//  public void testDecodeProtoYamlFile() throws IOException {
//    String yaml = ""
//        + "driverId: 1\n"
//        + "areaId: 2";
//    Path path = tempFolder.newFile("file.yaml").toPath();
//    try (BufferedWriter w = Files.newBufferedWriter(path)) {
//      w.write(yaml);
//    }
//
//    DriverArea da = ProtoUtil.decodeProtoYamlFile(path, DriverArea.getDefaultInstance());
//    assertEquals(1, da.getDriverId());
//    assertEquals(2, da.getAreaId());
//  }
//
//  @Test
//  @Ignore
//  // Uncomment this during manual testing, as we don't want running unit tests to require GCS access
//  public void testDecodeProtoYamlFileGCS() throws IOException {
//    String yaml = ""
//        + "driverId: 1\n"
//        + "areaId: 2";
//    Path path = PathUtil.getPath("gs://feast-temp/file.yaml");
//    log.info(path.toUri().toString());
//    try (BufferedWriter w = Files.newBufferedWriter(path)) {
//      w.write(yaml);
//    }
//
//    DriverArea da = ProtoUtil.decodeProtoYamlFile(path, DriverArea.getDefaultInstance());
//    assertEquals(1, da.getDriverId());
//    assertEquals(2, da.getAreaId());
//  }
//}