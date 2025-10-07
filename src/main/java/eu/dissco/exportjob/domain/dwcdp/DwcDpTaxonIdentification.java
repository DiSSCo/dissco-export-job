package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serial;
import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpTaxonIdentification implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;
  private String identificationID;
  private Integer taxonSortOrder;
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
}
