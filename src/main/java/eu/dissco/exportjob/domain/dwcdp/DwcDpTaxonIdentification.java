package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serial;
import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpTaxonIdentification implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;
    private String identificationID;
    private String taxonID;
    private Integer taxonOrder;
    private String higherClassificationName;
    private String higherClassificationRank;
    private String scientificName;
    private String taxonRank;
    private String taxonRemarks;
}
