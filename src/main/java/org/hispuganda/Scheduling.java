
package org.hispuganda;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.hispuganda.service.DHIS2Service;

// import io.quarkus.scheduler.Scheduled;
// import io.vertx.core.json.JsonArray;
// import io.vertx.core.json.JsonObject;

@ApplicationScoped
public class Scheduling {
  @Inject
  @RestClient
  DHIS2Service source;

  // @Scheduled(every = "40s")
  // void pullFromAPI() {
  //   DateTimeFormatter inBuiltFormatter2 = DateTimeFormatter.ISO_DATE_TIME;
  //   LocalDateTime now = LocalDateTime.now();
  //   String toDate = now.format(inBuiltFormatter2).split("[.]")[0];
  //   String fromDate = now.minusSeconds(40).format(inBuiltFormatter2).split("[.]")[0];
  //   System.out.println(fromDate);
  //   System.out.println(toDate);
  //   JsonObject dataElements = source.getDataElements("id,shortName,code", true, "domainType:eq:TRACKER");
  //   JsonArray data = dataElements.getJsonArray("dataElements");
  // }
}
