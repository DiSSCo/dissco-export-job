package eu.dissco.exportjob.domain.dwcdp;

import java.io.Serial;
import java.io.Serializable;
import lombok.Data;

@Data
public class DwcDpChronometricAgeAgent implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;
    private String chronometricAge_fk;
    private String agent_fk;
    private String agentRole;
    private String agentRoleIRI;
    private String agentRoleSource;
    private Integer agentRoleOrder;
    private String agentRoleDate;
}
