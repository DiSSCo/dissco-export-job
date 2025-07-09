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
  private String earliestEonOrLowestEonothem;
  private String latestEonOrHighestEonothem;
  private String earliestEraOrLowestErathem;
  private String latestEraOrHighestErathem;
  private String earliestPeriodOrLowestSystem;
  private String latestPeriodOrHighestSystem;
  private String earliestEpochOrLowestSeries;
  private String latestEpochOrHighestSeries;
  private String earliestAgeOrLowestStage;
  private String latestAgeOrHighestStage;
  private String lowestBiostratigraphicZone;
  private String highestBiostratigraphicZone;
  private String lithostratigraphicTerms;
  private String group;
  private String formation;
  private String member;
  private String bed;
}
