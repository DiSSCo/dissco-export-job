package eu.dissco.exportjob.repository;

import static eu.dissco.exportjob.database.jooq.Tables.SOURCE_SYSTEM;

import eu.dissco.exportjob.exceptions.FailedProcessingException;
import java.nio.charset.StandardCharsets;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class SourceSystemRepository {

  private static final String HANDLE_PROXY = "https://hdl.handle.net/";

  private final DSLContext context;

  private static String removeProxy(String id) {
    return id.replace(HANDLE_PROXY, "");
  }


  public String getEmlBySourceSystemId(String sourceSystemId) throws FailedProcessingException {
    var emlBytes = context.select(SOURCE_SYSTEM.EML).from(SOURCE_SYSTEM)
        .where(SOURCE_SYSTEM.ID.eq(removeProxy(sourceSystemId)))
        .fetchOne(SOURCE_SYSTEM.EML);
    if (emlBytes != null) {
      return new String(emlBytes, StandardCharsets.UTF_8);
    } else {
      throw new FailedProcessingException(
          "Failed to retrieve EML for source system: " + sourceSystemId);
    }
  }
}
