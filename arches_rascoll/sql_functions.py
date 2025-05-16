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