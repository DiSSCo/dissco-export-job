package eu.dissco.exportjob.domain;

import lombok.Getter;

@Getter
public enum JobStateEndpoint {
  FAILED("/failed"),
  RUNNING("/running");

  private final String endpoint;
  JobStateEndpoint(String s){
    this.endpoint = s;
  }
}
