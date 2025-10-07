package eu.dissco.exportjob.component;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.opencsv.exceptions.CsvRequiredFieldEmptyException;
import eu.dissco.exportjob.domain.dwcdp.DwcDpAgent;
import java.util.List;
import org.junit.jupiter.api.Test;

class CsvHeaderStrategyTest {

  @Test
  void testAddHeader() throws CsvRequiredFieldEmptyException {
    //Given
    var expected = List.of(
        "agentID", "agentRemarks", "agentType", "preferredAgentName");
    var csvHeaderStrategy = new CsvHeaderStrategy<>(DwcDpAgent.class, false);

    // When
    var result = csvHeaderStrategy.generateHeader(new DwcDpAgent());

    // Then
    assertArrayEquals(expected.toArray(new String[expected.size()]), result);
  }

  @Test
  void testNoHeader() throws CsvRequiredFieldEmptyException {
    //Given
    var csvHeaderStrategy = new CsvHeaderStrategy<>(DwcDpAgent.class, true);

    // When
    var result = csvHeaderStrategy.generateHeader(new DwcDpAgent());

    // Then
    assertEquals(0, result.length);
  }

}
