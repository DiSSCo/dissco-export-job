package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serial;
import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpEventAssertion implements Serializable {

  @Serial
  private static final long serialVersionUID = 1L;
  private String assertionID;
  private String eventID;
  private String verbatimAssertionType;
  private String assertionType;
  private String assertionTypeIRI;
  private String assertionTypeVocabulary;
  private String assertionMadeDate;
  private String assertionEffectiveDate;
  private String assertionValue;
  private String assertionValueIRI;
  private String assertionValueVocabulary;
  private Double assertionValueNumeric;
  private String assertionUnit;
  private String assertionUnitIRI;
  private String assertionUnitVocabulary;
  private String assertionBy;
  private String assertionByID;
  private String assertionProtocols;
  private String assertionProtocolID;
  private String assertionReferences;
  private String assertionRemarks;
}
