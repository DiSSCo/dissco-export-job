package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serial;
import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpAgent implements Serializable {

  @Serial
  private static final long serialVersionUID = 1L;
  private String agentID;
  private String agentType;
  private String agentTypeIRI;
  private String agentTypeVocabulary;
  private String preferredAgentName;
}
