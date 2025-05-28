package eu.dissco.exportjob.utils;


import static org.assertj.core.api.Assertions.assertThat;

import eu.dissco.exportjob.schema.OdsHasRole;
import org.junit.jupiter.api.Test;

class DwcDpUtilsTest {

  @Test
  void parseAgentDate() {
    // Given
    OdsHasRole odsHasRole = new OdsHasRole();
    odsHasRole.setSchemaStartDate("2023-01-01");
    odsHasRole.setSchemaEndDate("2023-12-31");

    // When
    String result = DwcDpUtils.parseAgentDate(odsHasRole);

    // Then
    assertThat(result).isEqualTo("2023-01-01/2023-12-31");
  }

  @Test
  void parseAgentDateWithOnlyStartDate() {
    // Given
    OdsHasRole odsHasRole = new OdsHasRole();
    odsHasRole.setSchemaStartDate("2023-01-01");

    // When
    String result = DwcDpUtils.parseAgentDate(odsHasRole);

    // Then
    assertThat(result).isEqualTo("2023-01-01");
  }

  @Test
  void parseAgentDateWithOnlyEndDate() {
    // Given
    OdsHasRole odsHasRole = new OdsHasRole();
    odsHasRole.setSchemaEndDate("2023-12-31");

    // When
    String result = DwcDpUtils.parseAgentDate(odsHasRole);

    // Then
    assertThat(result).isEqualTo("2023-12-31");
  }

  @Test
  void parseAgentDateEmpty() {
    // Given
    OdsHasRole odsHasRole = new OdsHasRole();

    // When
    String result = DwcDpUtils.parseAgentDate(odsHasRole);

    // Then
    assertThat(result).isNull();
  }
}
