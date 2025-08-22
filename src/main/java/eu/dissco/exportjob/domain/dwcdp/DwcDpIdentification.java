package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serial;
import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpIdentification implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;
    private String identificationID;
    private String basedOnOccurrenceID;
    private String basedOnMaterialEntityID;
    private String basedOnNucleotideSequenceID;
    private String basedOnNucleotideAnalysisID;
    private String basedOnMediaID;
    private String identificationType;
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
    private String higherClassificationName;
    private String higherClassificationRank;
    private String scientificName;
    private String taxonRank;
    private String taxonRemarks;
}
