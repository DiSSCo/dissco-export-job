package eu.dissco.exportjob.repository;

import eu.dissco.exportjob.Profiles;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Query;
import org.jooq.impl.DSL;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;

@Slf4j
@Repository
@Profile({Profiles.DWC_DP, Profiles.DWCA})
@RequiredArgsConstructor
public class DatabaseRepository {

  private final Field<String> uniqueIDField = DSL.field("id", String.class);
  private final Field<byte[]> dataField = DSL.field("data", byte[].class);

  private final DSLContext context;

  public void createTable(String tableName) {
    dropTable(tableName);
    context.createTable(tableName)
        .column(uniqueIDField)
        .column(dataField)
        .execute();
    context.createUniqueIndex().on(tableName, "id").execute();
  }

  public void dropTable(String tempTableName) {
    context.dropTableIfExists(tempTableName).execute();
  }

  public void insertRecords(String tableName, List<Pair<String, Object>> value) throws IOException {
    var queries = new ArrayList<Query>();
    for (Pair<String, Object> pair : value) {
      Query query = createQuery(tableName, pair);
      queries.add(query);
    }
    context.batch(queries).execute();
  }

  private Query createQuery(String tableName, Pair<String, Object> stringObjectPair)
      throws IOException {
    try (var baos = new ByteArrayOutputStream();
        var out = new ObjectOutputStream(baos)) {
      out.writeObject(stringObjectPair.getRight());
      return context.insertInto(DSL.table("\"" + tableName + "\""))
          .columns(uniqueIDField, dataField)
          .values(stringObjectPair.getLeft(), baos.toByteArray())
          .onConflictDoNothing();
    }
  }

  public List<byte[]> getRecords(String tableName, int start, int limit) {
    return context
        .select(dataField)
        .from(tableName)
        .offset(start)
        .limit(limit)
        .fetch().getValues(dataField);
  }
}
