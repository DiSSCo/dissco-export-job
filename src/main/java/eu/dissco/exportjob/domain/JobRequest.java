package eu.dissco.exportjob.domain;

import java.util.List;
import java.util.UUID;

public record JobRequest(
    List<SearchParam> searchParams,
    TargetType targetType,
    UUID jobId,
    Boolean isSourceSystemJob
) {

}
