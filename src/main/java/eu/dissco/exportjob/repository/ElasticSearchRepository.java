package eu.dissco.exportjob.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
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

  private static final String SORT_BY = "dcterms:identifier.keyword";
  private final ElasticsearchClient client;
  private final ElasticSearchProperties properties;

  private static List<Query> generateQuery(List<SearchParam> searchParams) {
    var qList = new ArrayList<Query>();
    for (var searchParam : searchParams) {
      var key = searchParam.inputField()
          .replace("'", "")
          .replace("[*]", "")
          .replace("$", "")
          .replace("[", "")
          .replace("]", ".")
          .replace("\"", "")
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

  public List<JsonNode> getTargetObjects(List<SearchParam> searchParams, TargetType targetType,
      String lastId, List<String> targetFields)
      throws IOException {
    var query = generateQuery(searchParams);
    var index = getIndex(targetType);

    return retrieveObjects(lastId, properties.getPageSize(), targetFields, index, query);
  }

  public List<JsonNode> getTargetForMediaList(List<String> mediaList) throws IOException {
    var queries = mediaList.stream().map(media -> new Query.Builder()
        .ids(id -> id.values(mediaList))
        .build()).toList();
    var index = properties.getDigitalMediaObjectIndex();
    return retrieveObjects(null, null, null, index, queries);
  }

  private List<JsonNode> retrieveObjects(String lastId, Integer pageSize, List<String> targetFields,
      String index, List<Query> query) throws IOException {
    var searchRequestBuilder = new SearchRequest.Builder()
        .index(index)
        .query(
            q -> q.bool(b -> b.must(query)))
        .trackTotalHits(t -> t.enabled(Boolean.TRUE))
        .size(properties.getPageSize())
        .sort(s -> s.field(f -> f.field(SORT_BY).order(SortOrder.Desc)));
    if (pageSize != null) {
      searchRequestBuilder.size(pageSize);
    }
    if (lastId != null) {
      searchRequestBuilder
          .searchAfter(sa -> sa.stringValue(lastId));
    }
    if (targetFields != null) {
      searchRequestBuilder
          .source(sourceConfig -> sourceConfig
              .filter(filter -> filter.includes(targetFields)));
    }
    var searchResult = client.search(searchRequestBuilder.build(), ObjectNode.class);
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

  public void shutdown() throws IOException {
    client._transport().close();
  }

}
