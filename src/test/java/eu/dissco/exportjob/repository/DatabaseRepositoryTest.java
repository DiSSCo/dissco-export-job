package eu.dissco.exportjob.repository;

import static eu.dissco.exportjob.utils.TestUtils.DOI_1;
import static eu.dissco.exportjob.utils.TestUtils.MAPPER;
import static eu.dissco.exportjob.utils.TestUtils.givenSpecimenJson;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DatabaseRepositoryTest extends BaseRepositoryIT {

  private DatabaseRepository repository;

  @BeforeEach
  void setUp() {
    repository = new DatabaseRepository(context);
  }

  @Test
  void testInsertRecords() throws IOException {
    // Given
    var specimenList = new ArrayList<Pair<String, Object>>();
    var expectedRecords = 10;
    for (int i = 0; i < expectedRecords; i++) {
      specimenList.add(Pair.of(DOI_1 + i, MAPPER.treeToValue(givenSpecimenJson(), Object.class)));
    }
    var tableName = "temp_table_640f3acb_material";
    repository.createTable(tableName);

    // When
    repository.insertRecords(tableName, specimenList);
    var records = repository.getRecords(tableName, 0, 10000);

    // Then
    assertThat(records).hasSize(expectedRecords);
  }

}
