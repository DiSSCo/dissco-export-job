package eu.dissco.exportjob.domain.dwcdp;

import lombok.Getter;

@Getter
public enum DwcDpClasses {
  AGENT("agent.csv", "agent", DwcDpAgent.class),
  AGENT_IDENTIFIER("agent-identifier.csv", "agent-identifier", DwcDpAgentIdentifier.class),
  EVENT("event.csv", "event", DwcDpEvent.class),
  EVENT_AGENT("event-agent-role.csv", "event-agent-role", DwcDpEventAgent.class),
  IDENTIFICATION("identification.csv", "identification", DwcDpIdentification.class),
  IDENTIFICATION_AGENT("identification-agent-role.csv", "identification-agent-role", DwcDpIdentificationAgent.class),
  MATERIAL("material.csv", "material", DwCDpMaterial.class),
  MATERIAL_IDENTIFIER("material-identifier.csv", "material-identifier", DwcDpMaterialIdentifier.class),
  MATERIAL_MEDIA("material-media.csv", "material-media", DwcDpMaterialMedia.class),
  MEDIA("media.csv", "media", DwcDpMedia.class),
  OCCURRENCE("occurrence.csv", "occurrence", DwcDpOccurrence.class),
  RELATIONSHIP("relationship.csv", "relationship", DwcDpRelationship.class),
  IDENTIFICATION_TAXON("identification-taxon.csv", "identification-taxon", DwcDpTaxonIdentification.class),
  MATERIAL_ASSERTION("material-assertion.csv", "material-assertion", DwcDpMaterialAssertion.class),
  EVENT_ASSERTION("event-assertion.csv", "event-assertion", DwcDpEventAssertion.class),
  MATERIAL_REFERENCE("material-reference.csv", "material-reference", DwcDpMaterialReference.class),
  REFERENCE("reference.csv", "reference", DwcDpReference.class),
  GEOLOGICAL_CONTEXT("geological-context.csv", "geological-context", DwcDpGeologicalContext.class),
  CHRONOMETRIC_AGE("chronometric-age.csv", "chronometric-age", DwcDpChronometricAge.class),
  CHRONOMETRIC_AGE_AGENT("chronometric-age-agent-role.csv", "chronometric-age-agent-role", DwcDpChronometricAgeAgent.class);
  
  
  private final String fileName;
  private final String className;
  private final Class<?> clazz;

  DwcDpClasses(String fileName, String className, Class<?> clazz) {
    this.fileName = fileName;
    this.className = className;
    this.clazz = clazz;
  }
}
