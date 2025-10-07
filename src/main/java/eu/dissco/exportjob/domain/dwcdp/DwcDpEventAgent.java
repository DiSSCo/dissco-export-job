package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serial;
import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpEventAgent implements Serializable {

  @Serial
  private static final long serialVersionUID = 1L;
  private String agentID;
  private String eventID;
  private String agentRole;
  private String agentRoleIRI;
  private String agentRoleSource;
  private Integer agentRoleOrder;
  private String agentRoleDate;
}
