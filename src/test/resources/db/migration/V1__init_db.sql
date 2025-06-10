create type translator_type as enum ('biocase', 'dwca');

create table source_system
(
    id text not null
        primary key,
    version integer default 1 not null,
    name text not null,
    endpoint text not null,
    created timestamp with time zone not null,
    modified timestamp with time zone not null,
    tombstoned timestamp with time zone,
    mapping_id text not null,
    creator text not null,
    translator_type translator_type not null,
    data jsonb not null,
    dwc_dp_link text,
    dwca_link text,
    eml bytea
);