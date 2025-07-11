package eu.dissco.exportjob.utils;

import eu.dissco.exportjob.exceptions.FailedProcessingException;
import eu.dissco.exportjob.schema.Agent;
import eu.dissco.exportjob.schema.Citation;
import eu.dissco.exportjob.schema.DigitalSpecimen;
import eu.dissco.exportjob.schema.Identifier;
import eu.dissco.exportjob.schema.OdsHasRole;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;

public class ExportUtils {

  public static final Set<String> EXCLUDE_IDENTIFIERS = Set.of("dwc:catalogNumber", "dwca:ID",
      "dwc:recordNumber", "dwc:occurrenceID", "abcd:recordURI", "abcd:unitID", "abcd:unitGUID");
  public static final Set<String> EXCLUDE_RELATIONSHIPS = Set.of("hasDigitalMedia",
      "hasOrganisationID", "hasSourceSystemID", "hasFDOType", "hasPhysicalIdentifier", "hasLicense",
      "hasCOLID", "hasCollectionID");

  private ExportUtils() {
    // Utility class
  }

  public static String parseAgentDate(OdsHasRole odsHasRole) {
    if (odsHasRole.getSchemaStartDate() != null
        && odsHasRole.getSchemaEndDate() != null) {
      return odsHasRole.getSchemaStartDate() + "/" + odsHasRole.getSchemaEndDate();
    } else if (odsHasRole.getSchemaStartDate() != null) {
      return odsHasRole.getSchemaStartDate();
    } else {
      return odsHasRole.getSchemaEndDate();
    }
  }

  public static String retrieveIdentifier(DigitalSpecimen digitalSpecimen,
      List<String> identifierTitles) {
    for (var identifierTitle : identifierTitles) {
      if (digitalSpecimen.getOdsHasIdentifiers() != null) {
        for (Identifier identifier : digitalSpecimen.getOdsHasIdentifiers()) {
          if (Objects.equals(identifierTitle, identifier.getDctermsTitle())) {
            return identifier.getDctermsIdentifier();
          }
        }
      }
    }
    return null;
  }

  public static String retrieveCombinedAgentId(List<Agent> odsHasAgents, String roleName) {
    if (odsHasAgents == null || odsHasAgents.isEmpty()) {
      return null;
    }
    List<String> agentIds = new ArrayList<>();
    for (var agent : odsHasAgents) {
      if (roleName == null || agent.getOdsHasRoles().stream()
          .anyMatch(role -> role.getSchemaRoleName().equals(roleName))) {
        String agentId = agent.getSchemaIdentifier();
        if (agentId != null && !agentId.isEmpty()) {
          agentIds.add(agentId);
        }
      }
    }
    return String.join(" | ", agentIds);
  }

  public static String retrieveCombinedAgentName(List<Agent> odsHasAgents, String roleName) {
    if (odsHasAgents == null || odsHasAgents.isEmpty()) {
      return null;
    }
    List<String> agentNames = new ArrayList<>();
    for (var agent : odsHasAgents) {
      if (roleName == null || agent.getOdsHasRoles().stream()
          .anyMatch(role -> role.getSchemaRoleName().equals(roleName))) {
        String agentName = agent.getSchemaName();
        if (agentName != null && !agentName.isEmpty()) {
          agentNames.add(agentName);
        }
      }
    }
    return String.join(" | ", agentNames);
  }


  public static String retrieveCombinedCitation(List<Citation> odsHasCitation) {
    if (odsHasCitation == null || odsHasCitation.isEmpty()) {
      return null;
    }
    return odsHasCitation.stream()
        .map(Citation::getDctermsBibliographicCitation)
        .filter(Objects::nonNull)
        .collect(Collectors.joining(" | "));
  }

  public static String retrieveTerm(DigitalSpecimen digitalSpecimen,
      Pair<Predicate<DigitalSpecimen>, Function<DigitalSpecimen, Object>> functions,
      String methodName)
      throws FailedProcessingException {
    if (functions.getLeft().test(digitalSpecimen)) {
      return null;
    }
    var classInstance = functions.getRight().apply(digitalSpecimen);
    return retrieveValueFromClass(classInstance, methodName);
  }

  private static String retrieveValueFromClass(Object classInstance, String methodName)
      throws FailedProcessingException {
    try {
      var method = classInstance.getClass().getMethod(methodName);
      var result = method.invoke(classInstance);
      if (result != null) {
        return String.valueOf(result);
      }
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new FailedProcessingException("Failed to retrieve value from class", e);
    }
    return null;
  }

  public static String convertValueToString(Object value) {
    if (value == null) {
      return null;
    }
    return String.valueOf(value);
  }

}
