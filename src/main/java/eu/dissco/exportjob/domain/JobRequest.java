package eu.dissco.exportjob.domain;

import java.util.List;
import java.util.UUID;

public record JobRequest(
    JobType jobType,
    List<SearchParam> searchParams,
    TargetType targetType,
    UUID jobId
) {

}
