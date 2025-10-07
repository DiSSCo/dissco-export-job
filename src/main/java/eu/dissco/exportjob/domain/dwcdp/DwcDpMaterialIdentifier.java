package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serial;
import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpMaterialIdentifier implements Serializable {

  @Serial
  private static final long serialVersionUID = 1L;
  private String identifier;
  private String materialEntityID;
  private String identifierType;
  private String identifierTypeIRI;
  private String identifierTypeSource;
  private String identifierLanguage;
}
