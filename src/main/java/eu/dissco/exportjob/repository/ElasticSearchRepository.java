package eu.dissco.exportjob.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.CountRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.dissco.exportjob.domain.SearchParam;
import eu.dissco.exportjob.domain.TargetType;
import eu.dissco.exportjob.properties.ElasticSearchProperties;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

@Slf4j
@Repository
@RequiredArgsConstructor
public class ElasticSearchRepository {
  private final ElasticsearchClient client;
  private final ElasticSearchProperties properties;

  public Long getTotalHits(List<SearchParam> searchParams, TargetType targetType)
      throws IOException {
    var query = generateQuery(searchParams);
    var index = getIndex(targetType);
    var countRequest = new CountRequest.Builder()
        .index(index)
        .query(
            q -> q.bool(b -> b.must(query)))
        .build();
    return client.count(countRequest).count();
  }

  public List<JsonNode> getTargetObjects(List<SearchParam> searchParams, TargetType targetType,
      int pageNumber)
      throws IOException {
    var query = generateQuery(searchParams);
    var index = getIndex(targetType);

    var searchRequest = new SearchRequest.Builder()
        .index(index)
        .query(
            q -> q.bool(b -> b.must(query)))
        .trackTotalHits(t -> t.enabled(Boolean.TRUE))
        .from(getOffset(pageNumber, properties.getPageSize()))
        .size(properties.getPageSize())
        .build();
    var searchResult = client.search(searchRequest, ObjectNode.class);

    return searchResult.hits().hits().stream()
        .map(Hit::source)
        .filter(Objects::nonNull)
        .map(JsonNode.class::cast)
        .toList();
  }

  private String getIndex(TargetType targetType) {
    return targetType == TargetType.DIGITAL_SPECIMEN ? properties.getDigitalSpecimenIndex()
        : properties.getDigitalMediaObjectIndex();
  }


  private static List<Query> generateQuery(List<SearchParam> searchParams) {
    var qList = new ArrayList<Query>();
    for (var searchParam : searchParams) {
      var key = searchParam.inputField()
          .replace("'", "")
          .replace("[*]", "")
          .replace("$", "")
          .replace("[", "")
          .replace("]", ".")
          + "keyword";
      var val = searchParam.inputValue();
      if (val == null || val.isEmpty()) {
        qList.add(
            new Query.Builder().bool(b -> b.mustNot(q -> q.exists(e -> e.field(key)))).build());
      } else {
        qList.add(
            new Query.Builder().term(t -> t.field(key).value(val).caseInsensitive(true)).build());
      }
    }
    return qList;
  }

  private static int getOffset(int pageNumber, int pageSize) {
    int offset = 0;
    if (pageNumber > 1) {
      offset = offset + (pageSize * (pageNumber - 1));
    }
    return offset;
  }

}
