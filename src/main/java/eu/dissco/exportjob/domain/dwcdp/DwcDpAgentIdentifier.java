package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serial;
import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpAgentIdentifier implements Serializable {

  @Serial
  private static final long serialVersionUID = 1L;
  private String agent_fk;
  private String identifier;
  private String identifierType;
  private String identifierTypeIRI;
  private String identifierTypeSource;
  private String identifierLanguage;
}
