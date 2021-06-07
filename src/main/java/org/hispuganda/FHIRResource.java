package org.hispuganda;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.hispuganda.service.DHIS2Service;
import org.hispuganda.service.ElasticsearchService;
import org.jboss.logging.Logger;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

@Path("/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class FHIRResource {

  private static final String MAPPINGS = "mappings";

  private static final String DHIS2 = "DHIS2";

  private static final String SYSTEM = "system";

  private static final String ID_NAME_SHORT_NAME_DESCRIPTION = "id,name,shortName,description";

  private static final Logger LOG = Logger.getLogger(FHIRResource.class);


  @Inject
  @RestClient
  DHIS2Service dhis2Service;

  @Inject
  ElasticsearchService elasticsearchService;

  @Inject
  FHIRUnmarshall fhir;

  @POST
  @Path("/index")
  public JsonObject index(JsonObject data, @QueryParam("index") String index) throws IOException {
    elasticsearchService.index(data, index);
    return data;
  }

  @GET
  @Path("/get/{index}/{id}")
  public JsonObject get(@PathParam("index") String index, @PathParam("id") String id) throws IOException {
    return elasticsearchService.get(index, id);
  }

  @GET
  @Path("/testing")
  public JsonArray search(@QueryParam("index") String index, @QueryParam("search") String search) throws IOException {
    return elasticsearchService.searchAny(index, search);
  }

  @GET
  @Path("/concepts")
  public JsonArray searchConcepts(@QueryParam("index") String index, @QueryParam("q") String concept)
    throws IOException {
    return elasticsearchService.searchAny(index, concept);
  }

  @GET
  @Path("/searching")
  public JsonObject searchConcepts(@QueryParam("index") String index, @QueryParam(SYSTEM) String system,
                                   @QueryParam("code") String code) throws IOException {
    return elasticsearchService.searchConcept(index, system, code);
  }

  @GET
  @Path("/concepts/{id}")
  public JsonObject searchConcept(@PathParam("id") String id, @QueryParam("index") String index) throws IOException {
    return elasticsearchService.get(index, id);
  }

  @GET
  @Path("/search")
  public JsonArray search(@QueryParam("index") String index, @QueryParam("term") String term,
                          @QueryParam("search") String search) throws IOException {
    return new JsonArray(elasticsearchService.search(index, term, search));
  }

  @POST
  @Path("/synchronize")
  public JsonObject synchronize(JsonObject settings) {
    JsonObject dataElements = dhis2Service.getDataElements("id,name,shortName,description,valueType", false,
      "domainType:eq:TRACKER");
    JsonObject attributes = dhis2Service.getTrackedEntityAttributes("id,name,shortName,description,valueType,unique",
      false);

    JsonObject trackedEntityTypes = dhis2Service.getTrackedEntityTypes(ID_NAME_SHORT_NAME_DESCRIPTION, false);
    JsonObject programs = dhis2Service.getProgram(ID_NAME_SHORT_NAME_DESCRIPTION, false);
    JsonObject programStages = dhis2Service.getProgramStages("id,name,description,repeatable,program[id,name]", false);

    synchronizeAttributes(attributes);
    synchronizeDataElements(dataElements);
    synchronizeTrackedEntityTypes(trackedEntityTypes);
    synchronizePrograms(programs);
    synchronizeProgramStages(programStages);

    return new JsonObject().put("message", "Finished synchronizing");
  }

  private void synchronizeAttributes(JsonObject attributes) {
    attributes.getJsonArray("trackedEntityAttributes").forEach(x -> {
      JsonObject term = (JsonObject) x;
      JsonObject mapping = new JsonObject().put(SYSTEM, DHIS2).put("code", term.getString("id"));
      term.put(MAPPINGS, new JsonArray().add(mapping));
      term.put("identifier", term.getBoolean("unique"));
      term.put("type", "");
      try {
        elasticsearchService.index(term, "attributes");
      } catch (IOException e) {
        e.printStackTrace();
      }
    });
  }

  private void synchronizeDataElements(JsonObject dataElements) {
    dataElements.getJsonArray("dataElements").forEach(x -> {
      JsonObject term = (JsonObject) x;
      JsonObject mapping = new JsonObject().put(SYSTEM, DHIS2).put("code", term.getString("id"));
      term.put(MAPPINGS, new JsonArray().add(mapping));
      try {
        elasticsearchService.index(term, "concepts");
      } catch (IOException e) {
        e.printStackTrace();
      }
    });
  }

  private void synchronizeTrackedEntityTypes(JsonObject trackedEntityTypes) {
    trackedEntityTypes.getJsonArray("trackedEntityTypes").forEach(x -> {
      JsonObject term = (JsonObject) x;
      JsonObject mapping = new JsonObject().put(SYSTEM, DHIS2).put("code", term.getString("id"));
      term.put(MAPPINGS, new JsonArray().add(mapping));
      if (term.getString("name").equalsIgnoreCase("person") || term.getString("name").equalsIgnoreCase("case")) {
        term.put("type", "Person");
      }
      try {
        elasticsearchService.index(term, "entities");
      } catch (IOException e) {
        e.printStackTrace();
      }
    });

  }

  private void synchronizePrograms(JsonObject programs) {
    programs.getJsonArray("programs").forEach(x -> {
      JsonObject term = (JsonObject) x;
      JsonObject mapping = new JsonObject().put(SYSTEM, DHIS2).put("code", term.getString("id"));
      term.put(MAPPINGS, new JsonArray().add(mapping));
      JsonArray type = new JsonArray();
      term.put("type", type);
      try {
        elasticsearchService.index(term, "programs");
      } catch (IOException e) {
        e.printStackTrace();
      }
    });
  }

  private void synchronizeProgramStages(JsonObject programStages) {
    programStages.getJsonArray("programStages").forEach(x -> {
      JsonObject term = (JsonObject) x;
      JsonObject mapping = new JsonObject().put(SYSTEM, DHIS2).put("code", term.getString("id"));
      term.put(MAPPINGS, new JsonArray().add(mapping));
      try {
        elasticsearchService.index(term, "stages");
      } catch (IOException e) {
        e.printStackTrace();
      }
    });
  }

  @POST
  @Path("/fhir")
  public JsonObject insert(JsonObject resource) throws IOException {
    JsonObject obj = null;
    switch (resource.getString("resourceType")) {
      case "Patient":
        obj = fhir.unmarshallPatient(resource);
        break;
      case "EpisodeOfCare":
        obj = fhir.unmarshallEpisodeOfCare(resource);
        break;
      case "Encounter":
        obj = fhir.unmarshallEncounter(resource);
        break;
      case "Observation":
        obj = fhir.unmarshallObservation(resource);
        break;
      case "Bundle":
        obj = fhir.unmarshallBundle(resource);
        break;
      default:
        obj = new JsonObject().put("error", "Unsupported resource type " + resource.getString("resourceType"));
        break;
    }
    return obj;
  }

  @POST
  @Path("/start")
  public JsonArray insert(JsonArray indexes) {
    JsonArray responses = new JsonArray();
    indexes.forEach(index -> {
      try {
        elasticsearchService.index(index.toString());
        responses.add("OK");
      } catch (IOException e) {
        responses.add("Failed");
      }
    });
    return responses;
  }
}
