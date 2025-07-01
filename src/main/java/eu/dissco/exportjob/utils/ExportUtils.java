package eu.dissco.exportjob.utils;

import eu.dissco.exportjob.schema.Agent;
import eu.dissco.exportjob.schema.DigitalSpecimen;
import eu.dissco.exportjob.schema.Identifier;
import eu.dissco.exportjob.schema.OdsHasRole;
import java.util.ArrayList;
import java.util.List;

public class ExportUtils {

  public static final List<String> EXCLUDE_IDENTIFIERS = List.of("dwc:catalogNumber", "dwca:ID", "dwc:recordNumber", "dwc:occurrenceID",
      "dcterms:identifier", "ods:physicalSpecimenID");
  public static final List<String> EXCLUDE_RELATIONSHIPS = List.of("hasDigitalMedia", "hasOrganisationID", "hasSourceSystemID",
      "hasFDOType", "hasPhysicalIdentifier", "hasLicense", "hasCOLID", "hasCollectionID");

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

  public static String retrieveSpecificIdentifier(DigitalSpecimen digitalSpecimen, String identifierTitle) {
    return digitalSpecimen.getOdsHasIdentifiers().stream()
        .filter(identifier -> identifierTitle.equals(identifier.getDctermsTitle())).map(
            Identifier::getDctermsIdentifier).findFirst().orElse(null);
  }

  public static String retrieveAgentIds(List<Agent> odsHasAgents, String identifier) {
    if (odsHasAgents == null || odsHasAgents.isEmpty()) {
      return null;
    }
    List<String> agentIds = new ArrayList<>();
    for (Agent agent : odsHasAgents) {
      if (identifier == null || identifier.isEmpty() || agent.getOdsHasRoles().stream()
          .anyMatch(role -> role.getSchemaRoleName().equals(identifier))) {
        String agentId = agent.getSchemaIdentifier();
        if (agentId != null && !agentId.isEmpty()) {
          agentIds.add(agentId);
        }
      }
    }
    return String.join(" | ", agentIds);
  }

  public static String retrieveAgentNames(List<Agent> odsHasAgents, String identifier) {
    if (odsHasAgents == null || odsHasAgents.isEmpty()) {
      return null;
    }
    List<String> agentNames = new ArrayList<>();
    for (Agent agent : odsHasAgents) {
      if (identifier == null || identifier.isEmpty() || agent.getOdsHasRoles().stream()
          .anyMatch(role -> role.getSchemaRoleName().equals(identifier))) {
        String agentName = agent.getSchemaName();
        if (agentName != null && !agentName.isEmpty()) {
          agentNames.add(agentName);
        }
      }
    }
    return String.join(" | ", agentNames);
  }

}
