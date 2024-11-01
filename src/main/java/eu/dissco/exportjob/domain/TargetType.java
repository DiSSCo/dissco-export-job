package eu.dissco.exportjob.domain;

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public enum TargetType {

  DIGITAL_SPECIMEN(
      "https://doi.org/21.T11148/894b1e6cad57e921764e"),

  DIGITAL_MEDIA(
      "https://doi.org/21.T11148/bbad8c4e101e8af01115");

  @Getter
  private final String name;

  TargetType(String name) {
    this.name = name;
  }

  public static TargetType fromString(String s){
    if ("https://doi.org/21.T11148/894b1e6cad57e921764e".equals(s)){
      return DIGITAL_SPECIMEN;
    } else if ("https://doi.org/21.T11148/bbad8c4e101e8af01115".equals(s)){
      return DIGITAL_MEDIA;
    }
    log.error("Invalid target type {}", s);
    throw new IllegalArgumentException();
  }

}
