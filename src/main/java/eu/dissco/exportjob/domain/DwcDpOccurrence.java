package eu.dissco.exportjob.domain;

import java.io.Serial;
import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpOccurrence implements Serializable {
  
  @Serial
  private static final long serialVersionUID = 1L;
  private String occurrenceID;
  private String eventID;
  private String surveyTargetID;
  private String recordedBy;
  private String recordedByID;
  private String organismQuantity;
  private String organismQuantityType;
  private String sex;
  private String lifeStage;
  private String reproductiveCondition;
  private String caste;
  private String behavior;
  private String vitality;
  private String establishmentMeans;
  private String degreeOfEstablishment;
  private String pathway;
  private String occurrenceStatus;
  private String occurrenceReferences;
  private String informationWithheld;
  private String dataGeneralizations;
  private String occurrenceRemarks;
  private String organismID;
  private String organismScope;
  private String organismName;
  private String organismRemarks;
  private String verbatimIdentification;
  private String taxonFormula;
  private String identifiedBy;
  private String identifiedByID;
  private String dateIdentified;
  private String identificationReferences;
  private String identificationVerificationStatus;
  private String identificationRemarks;
  private String taxonID;
  private String higherClassificationName;
  private String higherClassificationRank;
  private String scientificName;
  private String taxonRank;
  private String taxonRemarks;

  public boolean isEmpty() {
    return occurrenceID == null && eventID == null && surveyTargetID == null &&
        recordedBy == null && recordedByID == null && organismQuantity == null &&
        organismQuantityType == null && sex == null && lifeStage == null &&
        reproductiveCondition == null && caste == null && behavior == null &&
        vitality == null && establishmentMeans == null && degreeOfEstablishment == null &&
        pathway == null && occurrenceStatus == null && occurrenceReferences == null &&
        informationWithheld == null && dataGeneralizations == null &&
        occurrenceRemarks == null && organismID == null && organismScope == null &&
        organismName == null && organismRemarks == null && verbatimIdentification == null &&
        taxonFormula == null && identifiedBy == null && identifiedByID == null &&
        dateIdentified == null && identificationReferences == null &&
        identificationVerificationStatus == null && identificationRemarks == null &&
        taxonID == null && higherClassificationName == null &&
        higherClassificationRank == null && scientificName == null &&
        taxonRank == null && taxonRemarks == null;
  }
}
