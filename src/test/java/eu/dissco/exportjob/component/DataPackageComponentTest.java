package eu.dissco.exportjob.component;

import static eu.dissco.exportjob.utils.TestUtils.MAPPER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

import eu.dissco.exportjob.domain.dwcdp.DwcDpClasses;
import eu.dissco.exportjob.exceptions.FailedProcessingException;
import freemarker.template.Configuration;
import freemarker.template.Template;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Set;
import javax.xml.stream.XMLInputFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.io.ClassPathResource;

@ExtendWith(MockitoExtension.class)
class DataPackageComponentTest {

  private final Configuration configuration = new Configuration(Configuration.VERSION_2_3_32);
  private final XMLInputFactory factory = XMLInputFactory.newFactory();
  private Template template;
  private DataPackageComponent dataPackageComponent;

  @BeforeEach
  void setUp() throws IOException {
    configuration.setDirectoryForTemplateLoading(new File("src/main/resources/templates/"));
    template = configuration.getTemplate("data-package.ftl");
    this.dataPackageComponent = new DataPackageComponent(template, factory, MAPPER);
  }

  @Test
  void testFormatDataPackage() throws IOException, FailedProcessingException {
    // Given
    var eml = Files.readString(new ClassPathResource("sample-eml.xml").getFile().toPath());

    // When
    var result = dataPackageComponent.formatDataPackage(eml, Set.of(
        DwcDpClasses.OCCURRENCE, DwcDpClasses.AGENT, DwcDpClasses.MATERIAL, DwcDpClasses.EVENT,
        DwcDpClasses.MATERIAL_MEDIA, DwcDpClasses.MEDIA));

    // Then
    var datapackageNode = MAPPER.readTree(result);
    assertThat(datapackageNode.get("title").asText()).isEqualTo(
        "Naturalis Biodiversity Center (NL) - Tunicata");
    assertThat(datapackageNode.get("licenses").get(0).get("title").asText())
        .isEqualTo("CC-BY-4.0");
    assertThat(datapackageNode.get("resources").size()).isEqualTo(6);
    assertThat(datapackageNode.get("description").asText()).isEqualTo(
        "Database contains specimen records from the Tunicata collection of the Naturalis Biodiversity Center (Leiden, Netherlands). These specimens originate from the collections of the National Museum of Natural History (RMNH; Rijksmuseum voor Natuurlijke Historie), later National Museum of Natural History, Naturalis in Leiden and of the former Zoological Museum Amsterdam (ZMA). On request more information can be provided.");
  }

  @Test
  void testFormatDataPackageXmlException() throws IOException {
    // Given
    var eml = Files.readString(new ClassPathResource("invalid-eml.xml").getFile().toPath());

    // When
    assertThrows(FailedProcessingException.class,
        () -> dataPackageComponent.formatDataPackage(eml, Set.of(
            DwcDpClasses.OCCURRENCE, DwcDpClasses.AGENT, DwcDpClasses.MATERIAL, DwcDpClasses.EVENT,
            DwcDpClasses.MATERIAL_MEDIA, DwcDpClasses.MEDIA)));
  }

}
