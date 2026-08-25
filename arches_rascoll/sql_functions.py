import os
import uuid as GenUUID

from arches_rascoll import general_configs


ARCHES_V8_RESOURCE_INSTANCE_FUNCTION_FIX = """
-- FUNCTION: public.__arches_instance_view_update()

-- DROP FUNCTION IF EXISTS public.__arches_instance_view_update();

CREATE OR REPLACE FUNCTION public.__arches_instance_view_update()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
                declare
                    view_namespace text;
                    model_id uuid;
                    instance_id uuid;
                    transaction_id uuid;
                    edit_type text;
                begin
                    view_namespace = format('%s.%s', tg_table_schema, tg_table_name);
                    select obj_description(view_namespace::regclass, 'pg_class') into model_id;
                    if (TG_OP = 'DELETE') then
                        delete from public.resource_instances where resourceinstanceid = old.resourceinstanceid;
                        insert into bulk_index_queue (resourceinstanceid, createddate)
                            values (old.resourceinstanceid, current_timestamp) on conflict do nothing;
                        insert into edit_log (
                            resourceclassid,
                            resourceinstanceid,
                            edittype,
                            timestamp,
                            note,
                            transactionid
                        ) values (
                            model_id,
                            old.resourceinstanceid,
                            'delete',
                            now(),
                            'loaded via SQL backend',
                            public.uuid_generate_v1mc()
                        );
                        return old;
                    else
                        instance_id = new.resourceinstanceid;
                        if instance_id is null then
                            instance_id = public.uuid_generate_v1mc();
                        end if;

                        if (new.transactionid is null) then
                            transaction_id = public.uuid_generate_v1mc();
                        else
                            transaction_id = new.transactionid;
                        end if;

                        if (TG_OP = 'UPDATE') then
                            edit_type = 'edit';
                            if (transaction_id = old.transactionid) then
                                transaction_id = public.uuid_generate_v1mc();
                            end if;
                            update public.resource_instances
                            set createdtime = new.createdtime,
                                legacyid = new.legacyid
                            where resourceinstanceid = instance_id;
                        elsif (TG_OP = 'INSERT') then
                            edit_type = 'create';
                            insert into public.resource_instances(
                                resourceinstanceid,
                                graphid,
                                legacyid,
                                createdtime,
								 resource_instance_lifecycle_state_id
                            ) values (
                                instance_id,
                                model_id,
                                new.legacyid,
                                now(),
								'f75bb034-36e3-4ab4-8167-f520cf0b4c58'::uuid
                            );
                        end if;
                        insert into bulk_index_queue (resourceinstanceid, createddate)
                            values (instance_id, current_timestamp) on conflict do nothing;
                        insert into edit_log (
                            resourceclassid,
                            resourceinstanceid,
                            edittype,
                            timestamp,
                            note,
                            transactionid
                        ) values (
                            model_id,
                            instance_id,
                            edit_type,
                            now(),
                            'loaded via SQL backend',
                            transaction_id
                        );
                        return new;
                    end if;
                end;
            
$BODY$;

ALTER FUNCTION public.__arches_instance_view_update()
    OWNER TO postgres;
"""




POSTGRESQL_PERFORMANCE_FIX = """
create or replace function __arches_tile_view_update() returns trigger as $$
declare
    view_namespace text;
    group_id uuid;
    graph_id uuid;
    parent_id uuid;
    tile_id uuid;
    transaction_id uuid;
    json_data json;
    old_json_data jsonb;
    edit_type text;
begin
    select graphid into graph_id from nodes where nodeid = group_id;
    view_namespace = format('%s.%s', tg_table_schema, tg_table_name);
    select obj_description(view_namespace::regclass, 'pg_class') into group_id;
    if (TG_OP = 'DELETE') then
        select tiledata into old_json_data from tiles where tileid = old.tileid;
        delete from resource_x_resource where tileid = old.tileid;
        delete from public.tiles where tileid = old.tileid;
        insert into bulk_index_queue (resourceinstanceid, createddate)
            values (old.resourceinstanceid, current_timestamp) on conflict do nothing;
        insert into edit_log (
            resourceclassid,
            resourceinstanceid,
            nodegroupid,
            tileinstanceid,
            edittype,
            oldvalue,
            timestamp,
            note,
            transactionid
        ) values (
            graph_id,
            old.resourceinstanceid,
            group_id,
            old.tileid,
            'tile delete',
            old_json_data,
            now(),
            'loaded via SQL backend',
            public.uuid_generate_v1mc()
        );
        return old;
    else
        select __arches_get_json_data_for_view(new, tg_table_schema, tg_table_name) into json_data;
        select __arches_get_parent_id_for_view(new, tg_table_schema, tg_table_name) into parent_id;
        tile_id = new.tileid;
        if (new.transactionid is null) then
            transaction_id = public.uuid_generate_v1mc();
        else
            transaction_id = new.transactionid;
        end if;

        if (TG_OP = 'UPDATE') then
            select tiledata into old_json_data from tiles where tileid = tile_id;
            edit_type = 'tile edit';
            if (transaction_id = old.transactionid) then
                transaction_id = public.uuid_generate_v1mc();
            end if;
            update public.tiles
            set tiledata = json_data,
                nodegroupid = group_id,
                parenttileid = parent_id,
                resourceinstanceid = new.resourceinstanceid
            where tileid = new.tileid;
        elsif (TG_OP = 'INSERT') then
            old_json_data = null;
            edit_type = 'tile create';
            if tile_id is null then
                tile_id = public.uuid_generate_v1mc();
            end if;
            insert into public.tiles(
                tileid,
                tiledata,
                nodegroupid,
                parenttileid,
                resourceinstanceid
            ) values (
                tile_id,
                json_data,
                group_id,
                parent_id,
                new.resourceinstanceid
            );
        end if;
        perform __arches_refresh_tile_resource_relationships(tile_id);
        insert into bulk_index_queue (resourceinstanceid, createddate)
            values (new.resourceinstanceid, current_timestamp) on conflict do nothing;
        insert into edit_log (
            resourceclassid,
            resourceinstanceid,
            nodegroupid,
            tileinstanceid,
            edittype,
            newvalue,
            oldvalue,
            timestamp,
            note,
            transactionid
        ) values (
            graph_id,
            new.resourceinstanceid,
            group_id,
            tile_id,
            edit_type,
            json_data::jsonb,
            old_json_data,
            now(),
            'loaded via SQL backend',
            transaction_id
        );
        return new;
    end if;
    end;
$$ language plpgsql;
"""

# Has part, part type, (component) nodegroup cardinality fix
UPDATE_NODE_GROUP_CARDINALITY = """

UPDATE node_groups
SET cardinality = 'n'
WHERE nodegroupid = '6ee83594-08e4-11f0-81c1-0275dc2ded29';

UPDATE node_groups
SET cardinality = 'n'
WHERE nodegroupid = '6ee83f62-08e4-11f0-81c1-0275dc2ded29';

"""

# Comment these out, so we don't change the cardinality of the node groups
UPDATE_COLOR_NODE_GROUP_CARDINALITY = """
/*
UPDATE node_groups
SET cardinality = 'n'
WHERE nodegroupid = '3aff54bc-0f3b-11f0-aa84-02460e9d2217';
*/


"""

DIASABLE_TRIGGERS_BEFORE_INSERTS = """
/*
ALTER TABLE TILES DISABLE TRIGGER __arches_check_excess_tiles_trigger;
ALTER TABLE TILES DISABLE TRIGGER __arches_trg_update_spatial_attributes;
*/
"""

REACTIVATE_TRIGGERS_AFTER_INSERTS = """
/*
ALTER TABLE TILES ENABLE TRIGGER __arches_check_excess_tiles_trigger;
ALTER TABLE TILES ENABLE TRIGGER __arches_trg_update_spatial_attributes;
*/
"""

POSTGRESQL_AFTER_ETL_FUNCTION = """
select * from refresh_geojson_geometries();
"""

NODE_SLUG_CHARACTER_FIX = """
UPDATE nodes
SET name = REPLACE(name, '_time-span_edtf', '_time_span_edtf')
WHERE name LIKE '%_time-span_edtf%';

UPDATE nodes
SET name = REPLACE(name, '_time-span_type', '_time_span_type')
WHERE name LIKE '%_time-span_type%';
"""

NODE_CARDINALITY_FIX = """
UPDATE node_groups
SET cardinality = 'n'
WHERE nodegroupid = '2924a04c-73c0-4d09-972e-089a6630e232';
"""





CONTROLLED_LIST_ITEM_JSON_FIX = """
create or replace function __arches_get_node_value_sql(node nodes) returns text as
$$
declare
    node_value_sql text;
    select_sql     text = '(t.tiledata->>%L)';
    datatype       text = 'text';
begin
    select_sql = format(select_sql, node.nodeid);
    if (node.config ->> 'pgDatatype' is not null) then
        datatype = node.config ->> 'pgDatatype';
    else
        case node.datatype
            when 'geojson-feature-collection' then datatype = 'geometry';
            when 'string' then datatype = 'jsonb';
            when 'number' then datatype = 'numeric';
            when 'boolean' then datatype = 'boolean';
            when 'resource-instance' then datatype = 'jsonb';
            when 'resource-instance-list' then datatype = 'jsonb';
            when 'annotation' then datatype = 'jsonb';
            when 'file-list' then datatype = 'jsonb';
            when 'url' then datatype = 'jsonb';
            when 'date' then datatype = 'timestamp';
            when 'node-value' then datatype = 'uuid';
            when 'domain-value' then datatype = 'uuid';
            when 'domain-value-list' then datatype = 'uuid[]';
            when 'concept' then datatype = 'uuid';
            when 'concept-list' then datatype = 'uuid[]';
            when 'reference' then datatype = 'jsonb';
            else datatype = 'text';
            end case;
    end if;
    case datatype
        when 'geometry' then select_sql = format('
                                st_collect(
                                    array(
                                        select st_transform(geom, 4326) from geojson_geometries
                                        where geojson_geometries.tileid = t.tileid and nodeid = %L
                                    )
                                )',
                                                 node.nodeid
                                          );
        when 'timestamp' then select_sql = format(
                'to_date(
                    t.tiledata->>%L::text,
                    %L
                )',
                node.nodeid,
                node.config ->> 'dateFormat'
                                           );
        when 'uuid[]' then select_sql = format('(
                                    CASE
                                        WHEN t.tiledata->>%1$L is null THEN null
                                        ELSE ARRAY(
                                            SELECT jsonb_array_elements_text(
                                                t.tiledata->%1$L
                                            )::uuid
                                        )
                                    END
                                )', node.nodeid
                                        );
        else null;
        end case;


    node_value_sql = format(
            '%s::%s as "%s"',
            select_sql,
            datatype,
            __arches_slugify(node.name)
                     );
    return node_value_sql;
end
$$ language plpgsql volatile;
"""


ADD_GRAPH_PUBLICATION_ID = """
UPDATE resource_instances
SET graphpublicationid = graphs.publicationid
FROM graphs
WHERE graphs.graphid = resource_instances.graphid
AND resource_instances.graphpublicationid is null;
"""


FIX_KO_REPORT_BUG = """
update cards_x_nodes_x_widgets set config = jsonb_set(
    config,
	'{defaultValue}',
	(config -> 'defaultValue')::jsonb - '__ko_mapping__',
	true
) where config -> 'defaultValue' ->> '__ko_mapping__' is not null;
"""