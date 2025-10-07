package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serial;
import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpIdentification implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;
  private String identificationID;
  private String materialEntityID;
  private String mediaID;
  private String nucleotideAnalysisID;
  private String nucleotideSequenceID;
  private String occurrenceID;
  private String organismID;
  private String verbatimIdentification;
  private Boolean isAcceptedIdentification;
  private String taxonFormula;
  private String typeStatus;
  private String typeDesignationType;
  private String identifiedBy;
  private String identifiedByID;
  private String dateIdentified;
  private String identificationReferences;
  private String taxonAssignmentMethod;
  private String identificationVerificationStatus;
  private String identificationRemarks;
  private String taxonID;
  private String scientificNameID;
  private String geoClassificationCode;
  private String geoName;
  private String scientificName;
  private String scientificNameAuthorship;
  private String vernacularName;
  private String taxonRank;
  private String externalClassificationSource;
  private String kingdom;
  private String phylum;
  private String clazz;
  private String order;
  private String family;
  private String subfamily;
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
