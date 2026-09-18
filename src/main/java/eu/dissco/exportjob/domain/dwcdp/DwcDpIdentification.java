package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serial;
import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpIdentification implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;
  private String identification_pk;
  private String identificationID;
  private String identificationType;
  private String identificationProtocol_fk;
  private String materialEntity_fk;
  private String media_fk;
  private String nucleotideAnalysis_fk;
  private String nucleotideSequence_fk;
  private String occurrence_fk;
  private String organism_fk;
  private String verbatimIdentification;
  private Boolean isAcceptedIdentification;
  private String taxonFormula;
  private String typeStatus;
  private String typeDesignationType;
  private String identifiedBy;
  private String identifiedBy_fk;
  private String identifiedByID;
  private String dateIdentified;
  private String identificationReferences;
  private String identificationVerificationStatus;
  private String identificationRemarks;
  private String taxonID;
  private String scientificNameID;
  private String geologicalClassificationCodes;
  private String geologicalMaterialNames;
  private String scientificName;
  private String scientificNameAuthorship;
  private String vernacularName;
  private String taxonRank;
  private String classificationSystem;
  private String kingdom;
  private String phylum;
  private String clazz;
  private String order;
  private String superfamily;
  private String family;
  private String subfamily;
  private String tribe;
  private String subtribe;
  private String genus;
  private String genericName;
  private String subgenus;
  private String infragenericEpithet;
  private String specificEpithet;
  private String infraspecificEpithet;
  private String cultivarEpithet;
  private String nameAccordingTo;
  private String nomenclaturalCode;
  private String nomenclaturalStatus;
  private String namePublishedIn;
  private String namePublishedInYear;
  private String taxonRemarks;
  private String feedbackURL;
}
