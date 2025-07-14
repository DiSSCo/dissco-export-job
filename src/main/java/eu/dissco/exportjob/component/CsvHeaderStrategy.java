package eu.dissco.exportjob.component;

// 2. mappingStrategy

import com.opencsv.bean.HeaderColumnNameTranslateMappingStrategy;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;
import java.lang.reflect.Field;
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

  private final boolean headerPresent;

  public CsvHeaderStrategy(Class<T> type, boolean headerPresent) {
    this.headerPresent = headerPresent;
    Map<String, String> map = new HashMap<>();
    for (Field field : type.getDeclaredFields()) {
      map.put(field.getName(), field.getName());
    }
    setType(type);
    setColumnMapping(map);
  }

  @Override
  public String[] generateHeader(T bean) throws CsvRequiredFieldEmptyException {
    String[] result = super.generateHeader(bean);
    for (int i = 0; i < result.length; i++) {
      result[i] = getColumnName(i);
    }
    if (headerPresent) {
      return new String[0];
    } else {
      return result;
    }
  }

}
