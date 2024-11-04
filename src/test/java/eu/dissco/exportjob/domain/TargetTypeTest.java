package eu.dissco.exportjob.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import org.junit.jupiter.api.Test;

class TargetTypeTest {

  @Test
  void testFromStringMedia() {
    // When
    var type = TargetType.fromString("https://doi.org/21.T11148/bbad8c4e101e8af01115");

    // Then
    assertThat(type).isEqualTo(TargetType.DIGITAL_MEDIA);
  }

  @Test
  void testInvalidFromString() {
    // When / Then
    assertThrowsExactly(IllegalArgumentException.class, () -> TargetType.fromString("bad type"));
  }

}
