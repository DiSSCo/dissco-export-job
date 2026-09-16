package eu.dissco.exportjob.component;

import com.opencsv.bean.HeaderColumnNameTranslateMappingStrategy;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * Support for not writing a header on appending to the csv is not part of openCSV. This is why we
 * need to implement our own mapping strategy. On stackoverflow I found a solution that allows to
 * write a header only once This also fixes the issue where headers are written in uppercase.
 *
 * @author Alexander Ryasnyanskiy
 * https://stackoverflow.com/questions/48922642/appending-to-csv-file-without-headers created on
 * 2022-06-10
 */
public class CsvHeaderStrategy<T> extends HeaderColumnNameTranslateMappingStrategy<T> {

  private static final Map<String, String> HEADER_OVERRIDES = Map.of(
      "clazz", "class",
      "sampleRate", "sample-rate"
  );

  private final boolean skipHeader;

  public CsvHeaderStrategy(Class<T> type, boolean skipHeader) {
    this.skipHeader = skipHeader;
    Map<String, String> map = new HashMap<>();
    var declaredOrder = new ArrayList<String>();
    for (Field field : type.getDeclaredFields()) {
      map.put(field.getName(), field.getName());
      // This sort is required to ensure that the order of the columns is in line with the schema.
      // The data package will be invalid if the order of the columns doesn't match the datapackage schema description.
      declaredOrder.add(field.getName().toUpperCase());
    }
    setType(type);
    setColumnMapping(map);
    setColumnOrderOnWrite(Comparator.comparingInt(declaredOrder::indexOf));
  }

  @Override
  public String[] generateHeader(T bean) throws CsvRequiredFieldEmptyException {
    String[] result = super.generateHeader(bean);
    for (int i = 0; i < result.length; i++) {
      var columnName = getColumnName(i);
      result[i] = HEADER_OVERRIDES.getOrDefault(columnName, columnName);
    }
    if (skipHeader) {
      return new String[0];
    } else {
      return result;
    }
  }

}
