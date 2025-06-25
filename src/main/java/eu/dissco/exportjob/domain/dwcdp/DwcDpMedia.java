package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serial;
import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpMedia implements Serializable {
  
  @Serial
  private static final long serialVersionUID = 1L;
  private String mediaID;
  private String mediaType;
  private String subtypeLiteral;
  private String subtype;
  private String title;
  private String modified;
  private String MetadataDate;
  private String metadataLanguageLiteral;
  private String metadataLanguage;
  private String commenterLiteral;
  private String commenter;
  private String comments;
  private String reviewerLiteral;
  private String reviewer;
  private String reviewerComments;
  private String available;
  private String hasServiceAccessPoint;
  private String rights;
  private String rightsIRI;
  private String Owner;
  private String UsageTerms;
  private String WebStatement;
  private String licenseLogoURL;
  private String Credit;
  private String attributionLogoURL;
  private String attributionLinkURL;
  private String fundingAttribution;
  private String fundingAttributionID;
  private String source;
  private String sourceIRI;
  private String creator;
  private String creatorIRI;
  private String providerLiteral;
  private String provider;
  private String metadataCreatorLiteral;
  private String metadataCreator;
  private String metadataProviderLiteral;
  private String metadataProvider;
  private String description;
  private String caption;
  private String language;
  private String languageIRI;
  private String tag;
  private String CreateDate;
  private String timeOfDay;
  private String captureDevice;
  private String resourceCreationTechnique;
  private String collectionCode;
  private String collectionID;
  private String derivedFrom;
  private String accessURI;
  private String format;
  private String formatIRI;
  private String variantLiteral;
  private String variant;
  private String variantDescription;
  private String furtherInformationURL;
  private String licensingException;
  private String serviceExpectation;
  private String hashFunction;
  private String hashValue;
  private String PixelXDimension;
  private String PixelYDimension;
  private String feedbackURL;
}
