package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serial;
import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpUsagePolicy implements Serializable {

  @Serial
  private static final long serialVersionUID = 1L;
  private String usagePolicyID;
  private String rights;
  private String rightsIRI;
  private String rightsHolder;
  private String rightsHolderID;
  private String owner;
  private String ownerID;
  private String usageTerms;
  private String webStatement;
  private String accessRights;
  private String license;
  private String licenseLogoURL;
  private String licensingException;
  private String credit;
  private String attributionLogoURL;
  private String attributionLinkURL;


  public boolean isEmpty() {
    return (rights == null || rights.isBlank()) &&
        (rightsIRI == null || rightsIRI.isBlank()) &&
        (rightsHolder == null || rightsHolder.isBlank()) &&
        (rightsHolderID == null || rightsHolderID.isBlank()) &&
        (owner == null || owner.isBlank()) &&
        (ownerID == null || ownerID.isBlank()) &&
        (usageTerms == null || usageTerms.isBlank()) &&
        (webStatement == null || webStatement.isBlank()) &&
        (accessRights == null || accessRights.isBlank()) &&
        (license == null || license.isBlank()) &&
        (licenseLogoURL == null || licenseLogoURL.isBlank()) &&
        (licensingException == null || licensingException.isBlank()) &&
        (credit == null || credit.isBlank()) &&
        (attributionLogoURL == null || attributionLogoURL.isBlank()) &&
        (attributionLinkURL == null || attributionLinkURL.isBlank());
  }
}
