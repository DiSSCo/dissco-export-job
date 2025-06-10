package eu.dissco.exportjob.exceptions;

public class FailedProcessingException extends Exception {
  public FailedProcessingException(){
    super();
  }

  public FailedProcessingException(String s){
    super(s);
  }

  public FailedProcessingException(String s, Exception e){
    super(s, e);
  }

}
