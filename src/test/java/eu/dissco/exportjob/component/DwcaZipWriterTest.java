package eu.dissco.exportjob.component;

import static eu.dissco.exportjob.utils.TestUtils.TEMP_FILE_NAME;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import eu.dissco.exportjob.configuration.TemplateConfiguration;
import eu.dissco.exportjob.properties.IndexProperties;
import freemarker.template.Configuration;
import freemarker.template.TemplateException;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.gbif.dwc.terms.AcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.DwcaTerm;
import org.gbif.dwc.terms.Term;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DwcaZipWriterTest {

  private DwcaZipWriter dwcaZipWriter;

  @BeforeEach
  void setup() throws IOException {
    var freemarker = new TemplateConfiguration(mock(Configuration.class)).metaTemplate();
    var indexProperties = mock(IndexProperties.class);
    given(indexProperties.getTempFileLocation()).willReturn(TEMP_FILE_NAME);
    dwcaZipWriter = new DwcaZipWriter(indexProperties, freemarker);
  }

  @AfterEach
  void tearDown() throws IOException {
    var file = new File(TEMP_FILE_NAME);
    Files.deleteIfExists(file.toPath());
  }

  @Test
  void testWrite() throws IOException, TemplateException {
    //Given
    var map = new HashMap<Term, List<List<Pair<Term, String>>>>();
    map.put(DwcTerm.Occurrence, List.of(List.of(Pair.of(DwcaTerm.ID, "12345"))));
    map.put(DwcTerm.Identification, List.of(List.of(Pair.of(DwcaTerm.ID, "12345"))));
    map.put(AcTerm.Multimedia, List.of(List.of(Pair.of(DwcaTerm.ID, "12345"),
        Pair.of(AcTerm.accessURI, "http://example.com/image.jpg"))));

    // When
    dwcaZipWriter.writeRecords(map);
    dwcaZipWriter.close();

    // Then
    try (var fileSystem = FileSystems.newFileSystem(new File(TEMP_FILE_NAME).toPath())) {
      assertTrue(Files.exists(fileSystem.getPath("meta.xml")));
      assertTrue(Files.exists(fileSystem.getPath("Occurrence.tsv")));
      assertTrue(Files.exists(fileSystem.getPath("Identification.tsv")));
      assertTrue(Files.exists(fileSystem.getPath("Multimedia.tsv")));
    }
  }


  @Test
  void testWriteIllegalArgument() {
    //Given
    var map = new HashMap<Term, List<List<Pair<Term, String>>>>();
    map.put(DwcTerm.Occurrence, List.of(List.of(Pair.of(DwcaTerm.ID, "12345")),
        List.of(Pair.of(DwcaTerm.ID, "67890"),
            Pair.of(DwcTerm.institutionID, "https://ror.org/xxx"))));

    // When
    assertThrows(IllegalArgumentException.class, () -> dwcaZipWriter.writeRecords(map));
  }

  @Test
  void testEmptyMapping() {
    //Given
    var map = new HashMap<Term, List<List<Pair<Term, String>>>>();
    map.put(DwcTerm.Occurrence, List.of(List.of(Pair.of(DwcaTerm.ID, "12345")),
        List.of(Pair.of(DwcaTerm.ID, "67890"),
            Pair.of(DwcTerm.institutionID, "https://ror.org/xxx"))));
    map.put(DwcTerm.Identification, List.of(List.of()));

    // When
    assertThrows(IllegalArgumentException.class, () -> dwcaZipWriter.writeRecords(map));
  }

}
