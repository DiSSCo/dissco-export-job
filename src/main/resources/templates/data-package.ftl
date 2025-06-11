{
  "profile" : "data-package",
  "title" : "${title}",
  "description" : "${description!}",
  <#if licenseName?? || identifier?? >
  "licenses" : [ {
    <#if licenseName??>
    "name" : "${licenseName!}",
    </#if>
    <#if identifier??>
    "title" : "${identifier!}"
    </#if>
  } ],
  </#if>
  "version" : "1",
  "created" : "${created}",
  "resources" : [
<#if agent??>
  {
    "name" : "agent",
    "profile" : "tabular-data-resource",
    "format" : "csv",
    "encoding" : "UTF-8",
    "schema" : "https://rs.gbif.org/sandbox/experimental/data-packages/dwc-dp/0.1/table-schemas/agent.json",
    "path" : "agent.csv"
  }
</#if>
<#if agent_identifier??>
  , {
    "name" : "agent-identifier",
    "profile" : "tabular-data-resource",
    "format" : "csv",
    "encoding" : "UTF-8",
    "schema" : "https://rs.gbif.org/sandbox/experimental/data-packages/dwc-dp/0.1/table-schemas/agent-identifier.json",
    "path" : "agent-identifier.csv"
  }
</#if>
<#if event??>
  , {
    "name" : "event",
    "profile" : "tabular-data-resource",
    "format" : "csv",
    "encoding" : "UTF-8",
    "schema" : "https://rs.gbif.org/sandbox/experimental/data-packages/dwc-dp/0.1/table-schemas/event.json",
    "path" : "event.csv"
  }
</#if>
<#if event_agent_role??>
  , {
    "name" : "event-agent-role",
    "profile" : "tabular-data-resource",
    "format" : "csv",
    "encoding" : "UTF-8",
    "schema" : "https://rs.gbif.org/sandbox/experimental/data-packages/dwc-dp/0.1/table-schemas/event-agent-role.json",
    "path" : "event-agent-role.csv"
  }
</#if>
<#if identification??>
  , {
    "name" : "identification",
    "profile" : "tabular-data-resource",
    "format" : "csv",
    "encoding" : "UTF-8",
    "schema" : "https://rs.gbif.org/sandbox/experimental/data-packages/dwc-dp/0.1/table-schemas/identification.json",
    "path" : "identification.csv"
  }
</#if>
<#if identification_agent_role??>
  , {
  "name" : "identification-agent-role",
  "profile" : "tabular-data-resource",
  "format" : "csv",
  "encoding" : "UTF-8",
  "schema" : "https://rs.gbif.org/sandbox/experimental/data-packages/dwc-dp/0.1/table-schemas/identification-agent-role.json",
  "path" : "identification-agent-role.csv"
  }
</#if>
<#if material??>
  , {
    "name" : "material",
    "profile" : "tabular-data-resource",
    "format" : "csv",
    "encoding" : "UTF-8",
    "schema" : "https://rs.gbif.org/sandbox/experimental/data-packages/dwc-dp/0.1/table-schemas/material.json",
    "path" : "material.csv"
  }
</#if>
<#if material_identifier??>
  , {
    "name" : "material-identifier",
    "profile" : "tabular-data-resource",
    "format" : "csv",
    "encoding" : "UTF-8",
    "schema" : "https://rs.gbif.org/sandbox/experimental/data-packages/dwc-dp/0.1/table-schemas/material-identifier.json",
    "path" : "material-identifier.csv"
  }
</#if>
<#if material_media??>
  , {
    "name" : "material-media",
    "profile" : "tabular-data-resource",
    "format" : "csv",
    "encoding" : "UTF-8",
    "schema" : "https://rs.gbif.org/sandbox/experimental/data-packages/dwc-dp/0.1/table-schemas/material-media.json",
    "path" : "material-media.csv"
  }
</#if>
<#if media??>
  , {
    "name" : "media",
    "profile" : "tabular-data-resource",
    "format" : "csv",
    "encoding" : "UTF-8",
    "schema" : "https://rs.gbif.org/sandbox/experimental/data-packages/dwc-dp/0.1/table-schemas/media.json",
    "path" : "media.csv"
  }
</#if>
<#if occurrence??>
  , {
    "name" : "occurrence",
    "profile" : "tabular-data-resource",
    "format" : "csv",
    "encoding" : "UTF-8",
    "schema" : "https://rs.gbif.org/sandbox/experimental/data-packages/dwc-dp/0.1/table-schemas/occurrence.json",
    "path" : "occurrence.csv"
  }
</#if>
<#if relationship??>
  , {
    "name" : "relationship",
    "profile" : "tabular-data-resource",
    "format" : "csv",
    "encoding" : "UTF-8",
    "schema" : "https://rs.gbif.org/sandbox/experimental/data-packages/dwc-dp/0.1/table-schemas/relationship.json",
    "path" : "relationship.csv"
  }
</#if>
<#if identification??>
  , {
  "name" : "identification-taxon",
  "profile" : "tabular-data-resource",
  "format" : "csv",
  "encoding" : "UTF-8",
  "schema" : "https://rs.gbif.org/sandbox/experimental/data-packages/dwc-dp/0.1/table-schemas/identification-taxon.json",
  "path" : "identification-taxon.csv"
  }
</#if>
],
  "contributors" : [ ],
  "sources" : [ ]
}