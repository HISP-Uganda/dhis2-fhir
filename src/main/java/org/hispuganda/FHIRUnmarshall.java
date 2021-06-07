package org.hispuganda;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.hispuganda.service.DHIS2Service;
import org.hispuganda.service.ElasticsearchService;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.EpisodeOfCare;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.jboss.logging.Logger;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

@ApplicationScoped
public class FHIRUnmarshall {

  private static final String ERROR = "error";

  private static final String VALUE = "value";

  private static final String REFERENCE = "reference";

  private static final String EVENT = "event";

  private static final String VALUE_TYPE = "valueType";

  private static final String YYYY_MM_DD = "yyyy-MM-dd";

  private static final Logger LOG = Logger.getLogger(FHIRUnmarshall.class);

  private static final String ENROLLMENT = "enrollment";

  private static final String PROGRAM = "program";

  private static final String PATIENTS = "patients";

  private static final String ENROLLMENTS = "enrollments";

  private static final String CODES = "codes";

  private static final String TRACKED_ENTITY_INSTANCE = "trackedEntityInstance";

  private static final String ORG_UNIT = "orgUnit";

  private static final String MAPPINGS = "mappings";

  private static final String ATTRIBUTE = "attribute";

  private static final String ATTRIBUTES = "attributes";

  private static final String ENCOUNTERS = "encounters";

  @Inject
  ElasticsearchService es;

  @Inject
  @RestClient
  DHIS2Service dhis2Service;

  FhirContext ctx = FhirContext.forR4();
  IParser parser = ctx.newJsonParser();

  JsonObject unmarshallPatient(JsonObject patient) {
    JsonObject trackedEntityInstance = new JsonObject();
    JsonArray patientIdentifiers = new JsonArray();
    Patient p = parser.parseResource(Patient.class, patient.encode());
    try {
      List<JsonObject> allAttributes = es.search(ATTRIBUTES);
      List<JsonObject> allEntities = es.search("entities", "type", "Person");

      List<JsonObject> identifiers = allAttributes.stream().parallel()
          .filter(predicate -> predicate.getBoolean("identifier")).collect(Collectors.toList());

      List<JsonObject> extensions = allAttributes.stream().parallel()
          .filter(predicate -> predicate.getString("type").equals("extension")).collect(Collectors.toList());

      Map<String, String> initial = new HashMap<>();

      if (p.getBirthDate() != null) {
        initial.put("birthDate", new SimpleDateFormat(YYYY_MM_DD).format(p.getBirthDate()));
      }

      if (p.getNameFirstRep() != null) {
        initial.put("family", p.getNameFirstRep().getFamily());
        initial.put("given", p.getNameFirstRep().getGivenAsSingleString());
      }

      if (p.getGender() != null) {
        initial.put("gender", p.getGender().getDisplay());
      }

      if (p.getTelecom() != null && !p.getTelecom().isEmpty()) {
        initial.put("telecom", p.getTelecomFirstRep().getValue());
      }

      if (p.getAddressFirstRep() != null) {
        initial.put("address", p.getAddressFirstRep().getText());
      }

      JsonArray attributes = new JsonArray();

      p.getIdentifier().forEach(i -> {
        Coding coding = i.getType().getCodingFirstRep();
        String code = coding.getCode();
        String system = coding.getSystem();
        String dhis2Code = searchOneJsonObject(identifiers, MAPPINGS, "code", "system", code, system);
        if (dhis2Code != null) {
          attributes.add(new JsonObject().put(ATTRIBUTE, dhis2Code).put(VALUE, i.getValue()));
          patientIdentifiers.add(i.getValue());
        }
      });

      if (p.getMaritalStatus() != null) {
        String code = p.getMaritalStatus().getCodingFirstRep().getDisplay();
        String dhis2Code = getDHIS2Code(allAttributes, "maritalStatus");
        if (dhis2Code != null) {
          attributes.add(new JsonObject().put(ATTRIBUTE, dhis2Code).put(VALUE, code));
        }
      }

      List.of("family", "given", "gender", "birthDate", "address", "telecom").forEach(a -> {
        String dhis2Code = getDHIS2Code(allAttributes, a);
        if (dhis2Code != null && initial.get(a) != null) {
          attributes.add(new JsonObject().put(ATTRIBUTE, dhis2Code).put(VALUE, initial.get(a)));
        }
      });

      p.getExtension().stream().parallel().forEach(x -> {
        String dhis2Code = searchOneJsonObject(extensions, MAPPINGS, "system", x.getUrl());
        String value = x.getValue().primitiveValue();
        LOG.info(value);
        if (value != null && dhis2Code != null) {
          attributes.add(new JsonObject().put(ATTRIBUTE, dhis2Code).put(VALUE, value));
        }
      });

      trackedEntityInstance.put(ATTRIBUTES, attributes);
      String orgUnit = searchOrganisation(p);
      if (orgUnit != null) {
        trackedEntityInstance.put(ORG_UNIT, orgUnit);
      }

      if (!allEntities.isEmpty()) {
        JsonObject person = allEntities.get(0);
        String dhis2Code = getDHIS2Code(person.getJsonArray(MAPPINGS));
        if (dhis2Code != null) {
          trackedEntityInstance.put("trackedEntityType", dhis2Code);
        }
      }

      if (!patientIdentifiers.isEmpty() && trackedEntityInstance.getString(ORG_UNIT) != null
          && trackedEntityInstance.getString("trackedEntityType") != null) {
        JsonObject searchedPatient = searchPatient(patientIdentifiers);
        String id;
        if (searchedPatient == null) {
          id = dhis2Service.getId().getJsonArray(CODES).getString(0);
        } else {
          id = searchedPatient.getString(TRACKED_ENTITY_INSTANCE);
        }
        trackedEntityInstance.put(TRACKED_ENTITY_INSTANCE, id);
        LOG.info(trackedEntityInstance.encode());
        JsonObject obj = dhis2Service.addTrackedEntityInstance(trackedEntityInstance);
        if (obj.getInteger("httpStatusCode") == 200 || obj.getInteger("httpStatusCode") == 409) {
          es.index(trackedEntityInstance.copy().put("id", trackedEntityInstance.getString(TRACKED_ENTITY_INSTANCE))
              .put(ATTRIBUTES, patientIdentifiers).put(ENROLLMENTS, new JsonArray()).put(ENCOUNTERS, new JsonArray()),
              PATIENTS);
        }
        return obj;
      }
    } catch (Exception e) {
      LOG.error(e.getMessage());
      e.printStackTrace();
    }
    return new JsonObject().put(ERROR,
        "either missing patient identifier or missing organisation or missing tracked entity type");
  }

  JsonObject unmarshallEpisodeOfCare(JsonObject episodeOfCare) {
    EpisodeOfCare eoc = parser.parseResource(EpisodeOfCare.class, episodeOfCare.encode());
    String program = searchEOC(eoc);
    if (program != null) {
      JsonObject patient = searchPatient(new JsonArray().add(eoc.getPatient().getIdentifier().getValue()));
      LOG.info(patient);
      if (patient != null && !patient.isEmpty()) {
        String start = new SimpleDateFormat(YYYY_MM_DD).format(eoc.getPeriod().getStart());
        String enrollment = dhis2Service.getId().getJsonArray(CODES).getString(0);
        JsonObject proposedEnrollment = new JsonObject().put(ENROLLMENT, enrollment)
            .put(TRACKED_ENTITY_INSTANCE, patient.getString(TRACKED_ENTITY_INSTANCE)).put("enrollmentDate", start)
            .put("incidentDate", start).put(PROGRAM, program).put(ORG_UNIT, patient.getString(ORG_UNIT));
        boolean insert = false;
        if (patient.getString(TRACKED_ENTITY_INSTANCE) != null) {
          JsonArray enrollments = patient.getJsonArray(ENROLLMENTS);
          if (enrollments == null || enrollments.isEmpty()) {
            insert = true;
            patient.getJsonArray(ENROLLMENTS).add(proposedEnrollment);
          } else {
            List<JsonObject> prev = enrollments.stream().map(JsonObject.class::cast)
                .filter(enroll -> enroll.getString("enrollmentDate").equals(start)
                    && enroll.getString(PROGRAM).equals(program)
                    && enroll.getString(ORG_UNIT).equals(patient.getString(ORG_UNIT)))
                .collect(Collectors.toList());
            if (prev.isEmpty()) {
              insert = true;
              patient.getJsonArray(ENROLLMENTS).add(proposedEnrollment);
            }
          }
          if (insert) {
            try {
              JsonObject enroll = dhis2Service.addEnrollments(proposedEnrollment);
              es.index(patient, PATIENTS);
              return enroll;
            } catch (IOException e) {
              return new JsonObject().put("response", e.getMessage());
            }
          } else {
            return new JsonObject().put(eoc.getPatient().getIdentifier().getValue(), "Duplicate enrollment");
          }
        } else {
          return new JsonObject().put(eoc.getPatient().getIdentifier().getValue(),
              "Entity with identifier " + eoc.getPatient().getIdentifier().getValue() + "not found");
        }
      } else {
        return new JsonObject().put(eoc.getPatient().getIdentifier().getValue(),
            "Entity with identifier " + eoc.getPatient().getIdentifier().getValue() + "not found");
      }
    }
    return new JsonObject().put(eoc.getPatient().getIdentifier().getValue(),
        "Entity with identifier " + eoc.getPatient().getIdentifier().getValue() + "not found");
  }

  JsonObject unmarshallEncounter(JsonObject encounter) {
    Encounter e = parser.parseResource(Encounter.class, encounter.encode());
    String system = e.getTypeFirstRep().getCodingFirstRep().getSystem();
    String code = e.getTypeFirstRep().getCodingFirstRep().getCode();
    String reference = e.getSubject().getIdentifier().getValue();
    String encounterId = e.getIdentifierFirstRep().getValue();
    JsonObject patient = searchPatient(new JsonArray().add(reference));
    if (encounterId != null) {
      try {
        JsonObject encounterType = es.searchConcept("stages", system, code);
        if (encounterType.getString("id") != null && patient != null
            && patient.getString(TRACKED_ENTITY_INSTANCE) != null && e.getPeriod().getStart() != null) {
          JsonArray encounters = patient.getJsonArray(ENCOUNTERS);
          String programStage = encounterType.getString("id");
          String program = encounterType.getJsonObject(PROGRAM).getString("id");
          JsonObject enrollment = patient.getJsonArray(ENROLLMENTS).stream().map(JsonObject.class::cast)
              .filter(en -> en.getString(PROGRAM).equals(program)).collect(Utils.toSingleton());
          if (enrollment.getString(TRACKED_ENTITY_INSTANCE) != null) {
            if ((encounters != null && !encounters.isEmpty() && encounterType.getBoolean("repeatable"))
                || encounters == null || encounters.size() == 0) {
              JsonObject event = new JsonObject().put(PROGRAM, program).put("programStage", programStage)
                  .put(EVENT, dhis2Service.getId().getJsonArray(CODES).getString(0))
                  .put(TRACKED_ENTITY_INSTANCE, enrollment.getString(TRACKED_ENTITY_INSTANCE))
                  .put(ORG_UNIT, enrollment.getString(ORG_UNIT)).put(ENROLLMENT, enrollment.getString(ENROLLMENT))
                  .put("eventDate", new SimpleDateFormat(YYYY_MM_DD).format(e.getPeriod().getStart()))
                  .put("dataValues", new JsonArray());
              JsonObject resp = dhis2Service.addEvent(event);
              patient.getJsonArray(ENCOUNTERS)
                  .add(event.copy().put(REFERENCE, encounterId).put("id", event.getString(EVENT)));
              es.index(patient, PATIENTS);
              return resp;
            } else {
              return new JsonObject().put("error", "There is a possibility that encounter supplied is a duplicate");
            }
          } else {
            return new JsonObject().put("error", "Tracked entity instance could not be found for supplied enrollment");
          }
        } else {
          return new JsonObject().put("error", "Encounter type, patient or period not found");
        }
      } catch (IOException e1) {
        LOG.error(e1.getMessage());
        return new JsonObject().put("error", e1.getMessage());
      }
    } else {
      return new JsonObject().put("error",
          "No identifier supplied for encounter each encounter should have an identifier");
    }
  }

  JsonObject unmarshallObservation(JsonObject obs) {
    Observation observation = parser.parseResource(Observation.class, obs.encode());
    String patient = observation.getSubject().getIdentifier().getValue();
    String encounter = observation.getEncounter().getIdentifier().getValue();

    String system = observation.getCode().getCodingFirstRep().getSystem();

    String code = observation.getCode().getCodingFirstRep().getCode();

    JsonObject searchedPatient = searchPatient(new JsonArray().add(patient));
    JsonObject response = new JsonObject();

    if (searchedPatient != null) {
      JsonArray dataValues = new JsonArray();
      JsonObject event = searchOne(searchedPatient.getJsonArray(ENCOUNTERS), REFERENCE, encounter);
      JsonObject concept = searchConcept("concepts", system, code);

      if (event != null && concept != null) {
        dataValues.add(new JsonObject().put("dataElement", concept.getString("uuid")).put(VALUE,
            observation.getValue().primitiveValue()));
        JsonObject eventCopy = event.copy();
        eventCopy.remove(REFERENCE);
        eventCopy.put("dataValues", dataValues);
        try {
          response = dhis2Service.addEvent(eventCopy, event.getString(EVENT), concept.getString("uuid"));
        } catch (Exception e) {
          LOG.error(e.getMessage());
        }
      } else {
        response.put(ERROR, "could not find encounter or mapping");
      }
    } else {
      response.put(ERROR, "could not find patient with identifier " + patient);
    }
    return response;
  }

  JsonObject unmarshallBundle(JsonObject bundle) {
    Bundle b = parser.parseResource(Bundle.class, bundle.encode());
    JsonArray response = new JsonArray();
    b.getEntry().stream().forEach(x -> {
      parser.encodeResourceToString(x.getResource());
      JsonObject resource = new JsonObject(parser.encodeResourceToString(x.getResource()));
      switch (x.getResource().fhirType()) {
        case "Patient":
          response.add(unmarshallPatient(resource));
          break;
        case "EpisodeOfCare":
          response.add(unmarshallEpisodeOfCare(resource));
          break;
        case "Encounter":
          response.add(unmarshallEncounter(resource));
          break;
        case "Observation":
          response.add(unmarshallObservation(resource));
          break;
        case "Bundle":
          response.add(unmarshallBundle(resource));
          break;
        default:
          break;
      }
    });
    return new JsonObject().put("responses", response);
  }

  private String getDHIS2Code(List<JsonObject> attributes, String attribute) {
    JsonObject id = searchOne(attributes, attribute, "type");
    if (id != null && !id.isEmpty()) {
      return getDHIS2Code(id.getJsonArray(MAPPINGS));
    }
    return null;
  }

  private JsonObject searchOne(List<JsonObject> attributes, String attribute, String search) {
    return attributes.stream().filter(identifier -> identifier.getString(search).equals(attribute)).findFirst()
        .orElse(null);
  }

  private JsonObject searchOne(JsonArray arr, String field, String search) {
    return arr.stream().map(JsonObject.class::cast).filter(identifier -> identifier.getString(field).equals(search))
        .findFirst().orElse(null);
  }

  private String searchOneJsonObject(List<JsonObject> attributes, String attribute, String attribute1,
      String attribute2, String value, String value1) {
    JsonObject a = attributes.stream()
        .filter(identifier -> identifier.getJsonArray(attribute).stream().map(JsonObject.class::cast)
            .anyMatch(predicate -> predicate.getString(attribute1).equals(value)
                && predicate.getString(attribute2).equals(value1)))
        .findFirst().orElse(new JsonObject());

    return getDHIS2Code(a.getJsonArray(MAPPINGS));
  }

  private String searchOneJsonObject(List<JsonObject> attributes, String attribute, String attribute1, String value) {
    JsonObject a = attributes.stream().filter(identifier -> identifier.getJsonArray(attribute).stream()
        .map(JsonObject.class::cast).anyMatch(predicate -> predicate.getString(attribute1).equals(value))).findFirst()
        .orElse(new JsonObject());
    return getDHIS2Code(a.getJsonArray(MAPPINGS));
  }

  private String searchOrganisation(Patient p) {
    String system = p.getManagingOrganization().getIdentifier().getSystem();
    String value = p.getManagingOrganization().getIdentifier().getValue();
    if (system != null && value != null) {
      try {
        JsonObject search = es.searchConcept("organisations", system, value);
        return getDHIS2Code(search.getJsonArray(MAPPINGS));
      } catch (IOException e) {
        return null;
      }
    }
    return null;
  }

  private JsonObject searchConcept(String index, String system, String code) {
    if (system != null && code != null) {
      try {
        JsonObject search = es.searchConcept(index, system, code);
        if (!search.isEmpty()) {
          return new JsonObject().put("uuid", getDHIS2Code(search.getJsonArray(MAPPINGS))).put(VALUE_TYPE,
              search.getString(VALUE_TYPE));
        }
      } catch (IOException e) {
        LOG.error(e.getMessage());
        return null;
      }
    }
    return null;
  }

  private String searchEOC(EpisodeOfCare eoc) {
    String system = eoc.getIdentifierFirstRep().getType().getCodingFirstRep().getSystem();
    String code = eoc.getIdentifierFirstRep().getType().getCodingFirstRep().getCode();
    if (system != null && code != null) {
      try {
        JsonObject search = es.searchConcept("programs", system, code);
        return getDHIS2Code(search.getJsonArray(MAPPINGS));
      } catch (IOException e) {
        LOG.error(e.getMessage());
      }
    }
    return null;
  }

  private String getDHIS2Code(JsonArray array) {
    if (array != null) {
      JsonObject obj = array.stream().map(JsonObject.class::cast)
          .filter(predicate -> predicate.getString("system").equals("DHIS2")).findFirst().orElse(null);
      if (obj != null && !obj.isEmpty()) {
        return obj.getString("code");
      }
    }
    return null;
  }

  private JsonObject searchPatient(JsonArray patientIdentifiers) {
    try {
      List<JsonObject> hits = es.search(PATIENTS, ATTRIBUTES, patientIdentifiers);
      if (!hits.isEmpty()) {
        return hits.get(0);
      }
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
    return null;
  }

}
