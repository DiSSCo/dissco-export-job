package eu.dissco.exportjob.utils;


import static eu.dissco.exportjob.service.DwcaService.EVENT_FUNCTIONS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import eu.dissco.exportjob.exceptions.FailedProcessingException;
import eu.dissco.exportjob.schema.Agent;
import eu.dissco.exportjob.schema.DigitalSpecimen;
import eu.dissco.exportjob.schema.Event;
import eu.dissco.exportjob.schema.Identifier;
import eu.dissco.exportjob.schema.OdsHasRole;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ExportUtilsTest {

  private static Identifier givenIdentifier(String id, String title) {
    return new Identifier()
        .withDctermsIdentifier(id)
        .withDctermsTitle(title);
  }

  private static Stream<Arguments> sourceRetrieveIdentifier() {
    return Stream.of(
        Arguments.of(List.of("dwc:catalogNumber"), "12345"),
        Arguments.of(List.of("abcd:guid", "dwc:catalogueNumber"), "7890"),
        Arguments.of(List.of("abcd:recordURI"), null)
    );
  }

  private static Stream<Arguments> sourceRetrieveAgentNames() {
    return Stream.of(
        Arguments.of(null, "collector", null),
        Arguments.of(List.of(), null, null),
        Arguments.of(generateAgentList(), "collector", "agent1"),
        Arguments.of(generateAgentList(), null, "agent1 | agent2")
    );
  }

  private static List<Agent> generateAgentList() {
    var agent1 = new Agent().withSchemaIdentifier("agent1").withSchemaName("agent1")
        .withOdsHasRoles(List.of(new OdsHasRole().withSchemaRoleName("collector")));
    var agent2 = new Agent().withSchemaIdentifier("agent2").withSchemaName("agent2");
    return List.of(agent1, agent2);
  }

  private static Stream<Arguments> sourceRetrieveTerm() {
    return Stream.of(
        Arguments.of(
            new DigitalSpecimen().withOdsHasEvents(List.of(new Event().withDwcSex("female"))),
            "getDwcSex", "female"),
        Arguments.of(new DigitalSpecimen(), "getDwcSex", null),
        Arguments.of(
            new DigitalSpecimen().withOdsHasEvents(List.of(new Event())), "getDwcSex", null),
        Arguments.of(
            new DigitalSpecimen().withOdsHasEvents(List.of(new Event().withDwcYear(1990))),
            "getDwcYear",
            "1990")
    );
  }

  @Test
  void parseAgentDate() {
    // Given
    OdsHasRole odsHasRole = new OdsHasRole();
    odsHasRole.setSchemaStartDate("2023-01-01");
    odsHasRole.setSchemaEndDate("2023-12-31");

    // When
    String result = ExportUtils.parseAgentDate(odsHasRole);

    // Then
    assertThat(result).isEqualTo("2023-01-01/2023-12-31");
  }

  @Test
  void parseAgentDateWithOnlyStartDate() {
    // Given
    OdsHasRole odsHasRole = new OdsHasRole();
    odsHasRole.setSchemaStartDate("2023-01-01");

    // When
    String result = ExportUtils.parseAgentDate(odsHasRole);

    // Then
    assertThat(result).isEqualTo("2023-01-01");
  }

  @Test
  void parseAgentDateWithOnlyEndDate() {
    // Given
    OdsHasRole odsHasRole = new OdsHasRole();
    odsHasRole.setSchemaEndDate("2023-12-31");

    // When
    String result = ExportUtils.parseAgentDate(odsHasRole);

    // Then
    assertThat(result).isEqualTo("2023-12-31");
  }

  @Test
  void parseAgentDateEmpty() {
    // Given
    OdsHasRole odsHasRole = new OdsHasRole();

    // When
    String result = ExportUtils.parseAgentDate(odsHasRole);

    // Then
    assertThat(result).isNull();
  }

  @ParameterizedTest
  @MethodSource("sourceRetrieveIdentifier")
  void testRetrieveIdentifier(List<String> identifierTitles, String expected) {
    // Given
    var digitalSpecimen = new DigitalSpecimen();
    digitalSpecimen.setOdsHasIdentifiers(
        List.of(givenIdentifier("12345", "dwc:catalogNumber"),
            givenIdentifier("7890", "abcd:guid")));

    // When
    var result = ExportUtils.retrieveIdentifier(digitalSpecimen, identifierTitles);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @ParameterizedTest
  @MethodSource("sourceRetrieveAgentNames")
  void testRetrieveCombinedAgentId(List<Agent> agents, String role, String expected) {
    // Given

    // When
    var result = ExportUtils.retrieveCombinedAgentId(agents, role);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @ParameterizedTest
  @MethodSource("sourceRetrieveAgentNames")
  void testRetrieveCombinedAgentName(List<Agent> agents, String role, String expected) {
    // Given

    // When
    var result = ExportUtils.retrieveCombinedAgentName(agents, role);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @ParameterizedTest
  @MethodSource("sourceRetrieveTerm")
  void testRetrieveTerm(DigitalSpecimen digitalSpecimen, String methodName, String expected)
      throws FailedProcessingException {
    // Given

    // When
    var result = ExportUtils.retrieveTerm(digitalSpecimen, EVENT_FUNCTIONS, methodName);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testRetrieveTermException() {
    // Given

    // When / Then
    assertThrows(FailedProcessingException.class,
        () -> ExportUtils.retrieveTerm(new DigitalSpecimen().withOdsHasEvents(List.of(new Event())),
            EVENT_FUNCTIONS, "unknownMethod"));

  }

}
