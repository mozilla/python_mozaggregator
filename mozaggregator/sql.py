query = r"""
create or replace function aggregate_table_name(prefix text, channel text, version text, date text) returns text as $$
begin
    return format('%s_%s_%s_%s', prefix, channel, version, date);
end
$$ language plpgsql strict immutable;


create or replace function aggregate_arrays(acc bigint[], x bigint[]) returns bigint[] as $$
begin
    return (select array(
                select sum(elem)
                from (values (1, acc), (2, x)) as t(idx, arr)
                     , unnest(t.arr) with ordinality x(elem, rn)
                group by rn
                order by rn));
end
$$ language plpgsql strict immutable;


drop aggregate if exists aggregate_histograms(bigint[]);
create aggregate aggregate_histograms (bigint[]) (
    sfunc = aggregate_arrays, stype = bigint[], initcond = '{}'
);


create or replace function merge_table(prefix text, channel text, version text, date text, stage_table regclass) returns void as $$
declare
    tablename text;
    table_exists bool;
begin
    tablename := aggregate_table_name(prefix, channel, version, date);
    -- Check if table exists and if not create one
    table_exists := (select exists (select 1 from information_schema.tables where table_schema = 'public' and table_name = tablename));

    if not table_exists then
        execute format('create table %s as table %s', tablename, stage_table);
        execute format('create index on %s using GIN (dimensions jsonb_path_ops)', tablename);
        perform update_filter_options(channel, version, stage_table);
        return;
    end if;

    -- Update existing tuples and delete matching rows from the staging table
    execute 'with merge as (update ' || tablename || ' as dest
                            set histogram = aggregate_arrays(dest.histogram, src.histogram)
                            from ' || stage_table || ' as src
                            where dest.dimensions = src.dimensions
                            returning dest.*)
                  delete from ' || stage_table || ' as stage
                  using merge
                  where stage.dimensions = merge.dimensions';

    -- Insert new tuples
    execute 'insert into ' || tablename || ' (dimensions, histogram)
             select dimensions, histogram from ' || stage_table;
    perform update_filter_options(channel, version, stage_table);
end
$$ language plpgsql strict;


create or replace function lock_transaction(prefix text, channel text, version text, date text) returns bigint as $$
declare
    table_name text;
    lock bigint;
begin
    table_name := aggregate_table_name(prefix, channel, version, date);
    lock := (select h_bigint(table_name));
    execute 'select pg_advisory_xact_lock($1)' using lock;
    return lock;
end
$$ language plpgsql strict;


create or replace function h_bigint(text) returns bigint as $$
    select ('x'||substr(md5($1),1,16))::bit(64)::bigint;
$$ language sql;


create or replace function create_temporary_table(prefix text, channel text, version text, date text) returns text as $$
declare
    tablename text;
begin
    tablename := aggregate_table_name('staging_' || prefix, channel, version, date);
    execute 'create temporary table ' || tablename || ' (dimensions jsonb, histogram bigint[]) on commit drop';
    return tablename;
end
$$ language plpgsql strict;


create or replace function was_processed(prefix text, channel text, version text, date text, submission_date text) returns boolean as $$
declare
    table_name text;
    was_processed boolean;
begin
    table_name := aggregate_table_name(prefix, channel, version, date);
    select exists(select 1
                  from table_update_dates as t
                  where t.tablename = table_name and submission_date = any(t.submission_dates))
                  into was_processed;

    if (was_processed) then
        return was_processed;
    end if;

    with upsert as (update table_update_dates
                    set submission_dates = submission_dates || submission_date
                    where tablename = table_name
                    returning *)
         insert into table_update_dates
         select * from (values (table_name, array[submission_date])) as t
         where not exists(select 1 from upsert);

    return was_processed;
end
$$ language plpgsql strict;


create or replace function get_metric(prefix text, channel text, version text, date text, dimensions jsonb) returns table(label text, histogram bigint[]) as $$
declare
    tablename text;
begin
    if not dimensions ? 'metric' then
        raise exception 'Missing metric field!';
    end if;

    tablename := aggregate_table_name(prefix, channel, version, date);

    return query execute
    E'select dimensions->>\'label\', aggregate_histograms(histogram)
        from ' || tablename || E'
        where dimensions @> $1
        group by dimensions->>\'label\''
        using dimensions;
end
$$ language plpgsql strict stable;


drop type if exists metric_type;
create type metric_type AS (label text, histogram bigint[]);

create or replace function batched_get_metric(prefix text, channel text, version text, dates text[], dimensions jsonb) returns table(date text, label text, histogram bigint[]) as $$
begin
    return query select t.date, (get_metric(prefix, channel, version, t.date, dimensions)::text::metric_type).*
                 from (select unnest(dates)) as t(date);
end
$$ language plpgsql strict;


create or replace function list_buildids(prefix text, channel text) returns table(version text, buildid text) as $$
begin
    return query execute
    E'select t.matches[2], t.matches[3] from
        (select regexp_matches(table_name::text, $3)
         from information_schema.tables
         where table_schema=\'public\' and table_type=\'BASE TABLE\' and table_name like $1 || $2
         order by table_name desc) as t (matches)'
       using prefix, '_' || channel || '%', '^' || prefix || '_([^_]+)_([0-9]+)_([0-9]+)$';
end
$$ language plpgsql strict;


create or replace function list_channels(prefix text) returns table(channel text) as $$
begin
    return query execute
    E'select distinct t.matches[1] from
        (select regexp_matches(table_name::text, $1 || \'_([^_]+)_([0-9]+)_([0-9]+)\')
         from information_schema.tables
         where table_schema=\'public\' and table_type=\'BASE TABLE\'
         order by table_name desc) as t (matches)'
       using prefix;
end
$$ language plpgsql strict;


create or replace function get_dimension_values(filter text, table_name regclass) returns table(option text) as $$
declare
begin
     -- TODO: os & osVersion should be merged into a single dimension...
     if (filter = 'osVersion') then
         return query execute
         E'select concat(t.os, \',\', t.version)
           from (select distinct dimensions->>\'os\', dimensions->>\'osVersion\'
                 from ' || table_name || E') as t(os, version)';
     else
         return query execute
         E'select distinct dimensions->>\'' || filter || E'\' from ' || table_name;
     end if;
end
$$ language plpgsql strict stable;


create or replace function update_filter_options(channel text, version text, stage_table regclass) returns void as $$
declare
    table_match text;
    dimension_sample jsonb;
    dimension text;
begin
    table_match := aggregate_table_name('*', channel, version, '*');

    execute 'select dimensions
             from ' || stage_table || '
             limit 1'
             into dimension_sample;

    perform lock_transaction('*', channel, version, '*');

    for dimension in select jsonb_object_keys(dimension_sample)
    loop
        if (dimension = 'label' or dimension = 'os') then
          continue;
        end if;

        execute E'with curr as (select value
                                from filter_options
                                where table_match = $1 and filter = $2),
                       new as (select get_dimension_values($2, $3)),
                       diff as (select * from new except select * from curr)
                       insert into filter_options (table_match, filter, value)
                              select $1, $2, t.value
                              from diff as t(value)'
        using table_match, dimension, stage_table;
    end loop;
end
$$ language plpgsql strict;


create or replace function get_filter_options(channel text, version text, dimension text) returns table(option text) as $$
declare
    match_table text;
begin
    match_table := aggregate_table_name('*', channel, version, '*');

    if dimension = 'os' then
        dimension := 'osVersion';
    end if;

    return query
    select value
    from filter_options
    where table_match = match_table and filter = dimension;
end
$$ language plpgsql strict;


create or replace function create_tables() returns void as $$
declare
    table_exists boolean;
begin
   table_exists := (select exists (select 1 from information_schema.tables where table_schema = 'public' and table_name = 'table_update_dates'));
   if (not table_exists) then
       create table table_update_dates (tablename text primary key, submission_dates text[]);
       create index on table_update_dates (tablename);
   end if;

   table_exists := (select exists (select 1 from information_schema.tables where table_schema = 'public' and table_name = 'filter_options'));
   if (not table_exists) then
       create table filter_options (id serial primary key, table_match text not null, filter text not null, value text not null);
       create index on filter_options (table_match);
   end if;
end
$$ language plpgsql strict;


select create_tables();
"""
