package org.hispuganda.service;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
// import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@ApplicationScoped

public class ElasticsearchService {
  // private static final Logger LOG = Logger.getLogger(ElasticsearchService.class);

  private static final String SEARCH = "/_search";
  private static final String QUERY = "query";
  private static final String MATCH = "match";
  private static final String SOURCE = "_source";
  @Inject
  RestClient restClient;

  public void index(JsonObject concept, String index) throws IOException {
    Request request = new Request("PUT", "/" + index + "/_doc/" + concept.getString("id"));
    request.setJsonEntity(concept.toString());
    restClient.performRequest(request);
  }

  public void index(String index) throws IOException {
    Request request = new Request("PUT", "/" + index);
    restClient.performRequest(request);
  }

  public JsonObject get(String index, String id) throws IOException {
    Request request = new Request("GET", "/" + index + "/_doc/" + id);
    Response response = restClient.performRequest(request);
    String responseBody = EntityUtils.toString(response.getEntity());
    JsonObject json = new JsonObject(responseBody);
    return json.getJsonObject(SOURCE);
  }

  public JsonArray searchAny(String index, String search) throws IOException {
    Request request = new Request("GET", "/" + index + SEARCH);
    JsonObject termJson = new JsonObject().put(QUERY, search);
    JsonObject queryString = new JsonObject().put("query_string", termJson);
    JsonObject queryJson = new JsonObject().put(QUERY, queryString).put("size", 100);

    request.setJsonEntity(queryJson.encode());

    Response response = restClient.performRequest(request);
    String responseBody = EntityUtils.toString(response.getEntity());

    JsonObject json = new JsonObject(responseBody);
    return json.getJsonObject("hits").getJsonArray("hits");
  }

  public JsonObject searchAnyOne(String index, String search) throws IOException {
    JsonObject termJson = new JsonObject().put(QUERY, search);
    JsonObject queryString = new JsonObject().put("query_string", termJson);
    JsonObject queryJson = new JsonObject().put(QUERY, queryString).put("size", 1);
    Request request = new Request("GET", "/" + index + SEARCH);
    request.setJsonEntity(queryJson.encode());

    Response response = restClient.performRequest(request);
    String responseBody = EntityUtils.toString(response.getEntity());

    JsonObject json = new JsonObject(responseBody);

    JsonArray result = json.getJsonObject("hits").getJsonArray("hits");
    if (result.isEmpty()) {
      return new JsonObject();
    }
    return result.getJsonObject(0);

  }

  public List<JsonObject> search(String index, String term, String match) throws IOException {
    JsonObject termJson = new JsonObject().put(term + ".keyword", match);
    JsonObject matchJson = new JsonObject().put(MATCH, termJson);
    JsonObject queryJson = new JsonObject().put(QUERY, matchJson).put("size", 10000);
    Request request = new Request("GET", "/" + index + SEARCH);
    request.setJsonEntity(queryJson.encode());
    Response response = restClient.performRequest(request);
    String responseBody = EntityUtils.toString(response.getEntity());
    JsonObject json = new JsonObject(responseBody);
    return json.getJsonObject("hits").getJsonArray("hits")
      .stream().parallel()
      .map(x -> ((JsonObject) x).getJsonObject(SOURCE))
      .collect(Collectors.toList());
  }

  public List<JsonObject> search(String index) throws IOException {
    JsonObject queryJson = new JsonObject()
      .put(QUERY, new JsonObject().put("match_all", new JsonObject()))
      .put("size", 1000);
    Request request = new Request("GET", "/" + index + SEARCH);
    request.setJsonEntity(queryJson.encode());
    Response response = restClient.performRequest(request);
    String responseBody = EntityUtils.toString(response.getEntity());
    JsonObject json = new JsonObject(responseBody);

    return json.getJsonObject("hits").getJsonArray("hits")
      .stream().parallel()
      .map(x -> ((JsonObject) x).getJsonObject(SOURCE))
      .collect(Collectors.toList());
  }

  public List<JsonObject> search(String index, String term, JsonArray match) throws IOException {
    Request request = new Request("GET", "/" + index + SEARCH);
    JsonObject term1 = new JsonObject().put("filter", new JsonObject().put("terms", new JsonObject().put(term + ".keyword", match)));
    JsonObject bool = new JsonObject().put("bool", term1);
    JsonObject queryJson = new JsonObject().put(QUERY, bool);
    request.setJsonEntity(queryJson.encode());
    Response response = restClient.performRequest(request);
    String responseBody = EntityUtils.toString(response.getEntity());
    JsonObject json = new JsonObject(responseBody);

    return json.getJsonObject("hits").getJsonArray("hits")
      .stream().parallel()
      .map(x -> ((JsonObject) x).getJsonObject(SOURCE))
      .collect(Collectors.toList());
  }

  public JsonObject searchOne(String index, String term, String match) throws IOException {
    Request request = new Request("GET", "/" + index + SEARCH);
    JsonObject termJson = new JsonObject().put(term, match);
    JsonObject matchJson = new JsonObject().put(MATCH, termJson);
    JsonObject queryJson = new JsonObject().put(QUERY, matchJson).put("size", 1);
    request.setJsonEntity(queryJson.encode());
    Response response = restClient.performRequest(request);
    String responseBody = EntityUtils.toString(response.getEntity());
    JsonObject json = new JsonObject(responseBody);
    JsonArray result = json.getJsonObject("hits").getJsonArray("hits");
    if (result.isEmpty()) {
      return new JsonObject();
    }
    return result.getJsonObject(0).getJsonObject(SOURCE, new JsonObject());
  }

  public JsonObject searchConcept(String index, String system, String code) throws IOException {
    try {
      JsonObject term1 = new JsonObject().put(MATCH, new JsonObject().put("mappings.system", system));
      JsonObject term2 = new JsonObject().put(MATCH, new JsonObject().put("mappings.code", code));
      JsonArray should = new JsonArray().add(term1).add(term2);
      JsonObject bool = new JsonObject().put("bool", new JsonObject().put("must", should));
      JsonObject queryJson = new JsonObject().put(QUERY, bool);
      Request request = new Request("GET", "/" + index + SEARCH);
      request.setJsonEntity(queryJson.encode());
      Response response = restClient.performRequest(request);
      String responseBody = EntityUtils.toString(response.getEntity());
      JsonObject json = new JsonObject(responseBody);
      JsonArray result = json.getJsonObject("hits").getJsonArray("hits");
      if (result.isEmpty()) {
        return new JsonObject();
      }
      return result.getJsonObject(0).getJsonObject(SOURCE, new JsonObject());

    } catch (Exception e) {
      return new JsonObject().put("error", e.getMessage());
    }
  }

  public JsonObject searchConcept(String index, JsonArray should) throws IOException {
    try {
      JsonObject bool = new JsonObject().put("bool", new JsonObject().put("must", should));
      JsonObject queryJson = new JsonObject().put(QUERY, bool);
      Request request = new Request("GET", "/" + index + SEARCH);
      request.setJsonEntity(queryJson.encode());
      Response response = restClient.performRequest(request);
      String responseBody = EntityUtils.toString(response.getEntity());
      JsonObject json = new JsonObject(responseBody);
      JsonArray result = json.getJsonObject("hits").getJsonArray("hits");
      if (result.isEmpty()) {
        return new JsonObject();
      }
      return result.getJsonObject(0).getJsonObject(SOURCE, new JsonObject());

    } catch (Exception e) {
      return new JsonObject().put("error", e.getMessage());
    }
  }
}
