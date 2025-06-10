package eu.dissco.exportjob.repository;

import static eu.dissco.exportjob.database.jooq.Tables.SOURCE_SYSTEM;
import static eu.dissco.exportjob.utils.TestUtils.SOURCE_SYSTEM_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

import eu.dissco.exportjob.database.jooq.enums.TranslatorType;
import eu.dissco.exportjob.exceptions.FailedProcessingException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import org.jooq.JSONB;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

class SourceSystemRepositoryTest extends BaseRepositoryIT {

  private SourceSystemRepository repository;

  @BeforeEach
  void setUp() {
    repository = new SourceSystemRepository(context);
  }

  @AfterEach
  void cleanup() {
    context.truncate(SOURCE_SYSTEM).execute();
  }

  @Test
  void testGetEmlBySourceSystemId() throws IOException, FailedProcessingException {
    // Given
    var expected = Files.readAllBytes(new ClassPathResource("sample-eml.xml").getFile().toPath());
    givenInsertRecords(expected);

    // When
    var result = repository.getEmlBySourceSystemId(SOURCE_SYSTEM_ID);

    // Then
    assertThat(result).isEqualTo(new String(expected, StandardCharsets.UTF_8));
  }

  @Test
  void testNoEmlFileForSourceSystem() {
    // Given
    givenInsertRecords(null);

    // When
    assertThrows(FailedProcessingException.class,
        () -> repository.getEmlBySourceSystemId(SOURCE_SYSTEM_ID));
  }

  private void givenInsertRecords(byte[] eml) {
    context.insertInto(SOURCE_SYSTEM)
        .set(SOURCE_SYSTEM.ID, "TEST/Z1M-8WG-DCD")
        .set(SOURCE_SYSTEM.NAME, "Naturalis Vermes Dataset")
        .set(SOURCE_SYSTEM.ENDPOINT, "https://example.com/endpoint")
        .set(SOURCE_SYSTEM.CREATOR, "e2befba6-9324-4bb4-9f41-d7dfae4a44b0")
        .set(SOURCE_SYSTEM.CREATED, Instant.parse("2022-09-16T08:25:01.00Z"))
        .set(SOURCE_SYSTEM.MODIFIED, Instant.parse("2022-09-16T08:25:01.00Z"))
        .set(SOURCE_SYSTEM.TRANSLATOR_TYPE, TranslatorType.dwca)
        .set(SOURCE_SYSTEM.MAPPING_ID, "20.5000.1025/GW0-POP-XAS")
        .set(SOURCE_SYSTEM.DATA, JSONB.valueOf("{}"))
        .set(SOURCE_SYSTEM.EML, eml)
        .execute();
  }
}
