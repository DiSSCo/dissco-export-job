package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serial;
import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpChronometricAge implements Serializable {

  @Serial
  private static final long serialVersionUID = 1L;
  private String chronometricAge_pk;
  private String chronometricAgeID;
  private String event_fk;
  private String verbatimChronometricAge;
  private String chronometricAgeProtocol;
  private String chronometricAgeProtocol_fk;
  private String uncalibratedChronometricAge;
  private String chronometricAgeConversionProtocol;
  private String chronometricAgeConversionProtocol_fk;
  private String earliestChronometricAge;
  private String earliestChronometricAgeReferenceSystem;
  private String latestChronometricAge;
  private String latestChronometricAgeReferenceSystem;
  private Integer chronometricAgeUncertaintyInYears;
  private String chronometricAgeUncertaintyMethod;
  private String materialDated;
  private String materialDated_fk;
  private String materialDatedID;
  private String materialDatedRelationship;
  private String chronometricAgeDeterminedBy;
  private String chronometricAgeDeterminedBy_fk;
  private String chronometricAgeDeterminedByID;
  private String chronometricAgeDeterminedDate;
  private String chronometricAgeReferences;
  private String chronometricAgeRemarks;
}
