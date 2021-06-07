package org.hispuganda.service;

import java.util.Base64;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

import io.vertx.core.json.JsonObject;

@Path("/")
@RegisterRestClient
@Produces("application/json")
@ClientHeaderParam(name = "Authorization", value = "{lookupAuth}")
public interface DHIS2Service {

  @GET
  @Path("dataElements")
  JsonObject getDataElements(@QueryParam("fields") String fields);

  @GET
  @Path("dataElements")
  public JsonObject getDataElements(@QueryParam("fields") String fields, @QueryParam("filter") String from);

  @GET
  @Path("dataElements")
  JsonObject getDataElements(@QueryParam("fields") String fields, @QueryParam("filter") String from,
      @QueryParam("filter") String to);

  @GET
  @Path("dataElements")
  JsonObject getDataElements(@QueryParam("fields") String fields, @QueryParam("paging") Boolean paging);

  @GET
  @Path("dataElements")
  JsonObject getDataElements(@QueryParam("fields") String fields, @QueryParam("paging") Boolean paging,
      @QueryParam("filter") String filter1);

  @GET
  @Path("organisationUnits")
  JsonObject getOrganisationUnits(@QueryParam("fields") String fields, @QueryParam("paging") Boolean paging,
      @QueryParam("level") int level);

  @GET
  @Path("trackedEntityAttributes")
  JsonObject getTrackedEntityAttributes(@QueryParam("fields") String fields, @QueryParam("paging") Boolean paging);

  @GET
  @Path("trackedEntityTypes")
  JsonObject getTrackedEntityTypes(@QueryParam("fields") String fields, @QueryParam("paging") Boolean paging);

  @GET
  @Path("programs")
  JsonObject getProgram(@QueryParam("fields") String fields, @QueryParam("paging") Boolean paging);

  @GET
  @Path("programStages")
  JsonObject getProgramStages(@QueryParam("fields") @DefaultValue("id,name,description,program") String fields,
      @QueryParam("paging") @DefaultValue("false") Boolean paging);

  @GET
  @Path("system/id")
  JsonObject getId();

  @POST
  @Path("trackedEntityInstances")
  JsonObject addTrackedEntityInstance(JsonObject instancce);

  @POST
  @Path("enrollments")
  JsonObject addEnrollments(JsonObject enrollments);

  @POST
  @Path("events")
  JsonObject addEvent(JsonObject events);

  @PUT
  @Path("events/{event}/{dataElement}")
  JsonObject addEvent(JsonObject events, @PathParam("event") String event,
      @PathParam("dataElement") String dataElement);

  default String lookupAuth() {
    String username = ConfigProvider.getConfig().getValue("dhis2.username", String.class);
    String password = ConfigProvider.getConfig().getValue("dhis2.password", String.class);
    String login = username + ":" + password;
    return "Basic " + Base64.getEncoder().encodeToString(login.getBytes());
  }

}
