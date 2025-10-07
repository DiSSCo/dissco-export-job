package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serial;
import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpGeologicalContext implements Serializable {

  @Serial
  private static final long serialVersionUID = 1L;
  private String geologicalContextID;
  private String eventID;
  private String earliestEonOrEonothem;
  private String earliestEraOrErathem;
  private String earliestPeriodOrSystem;
  private String earliestEpochOrSeries;
  private String earliestStageOrAge;
  private String earliestHighChronostratigraphicZone;
  private String latestEonOrEonothem;
  private String latestEraOrErathem;
  private String latestPeriodOrSystem;
  private String latestEpochOrSeries;
  private String latestStageOrAge;
  private String latestHighChronostratigraphicZone;
  private String lowestBiostratigraphicZone;
  private String highestBiostratigraphicZone;
  private String lithostratigraphicTerms;
  private String group;
  private String formation;
  private String member;
  private String bed;
}
