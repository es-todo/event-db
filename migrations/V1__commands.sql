create table command (
  command_uuid uuid primary key,
  command_type text not null,
  command_data jsonb not null,
  command_date timestamp without time zone not null default now(),
  command_auth jsonb not null
);

create table event (
  event_t bigint not null primary key,
  event_date timestamp without time zone not null default now(),
  event_data jsonb not null,
  command_uuid uuid not null,
  foreign key (command_uuid) references command (command_uuid),
  unique (command_uuid)
);

create table queue (
  command_uuid uuid primary key,
  foreign key (command_uuid) references command (command_uuid)
);

create type status_type_type as enum ('queued', 'succeeded', 'failed', 'aborted');

create table status (
  status_t bigint not null primary key,
  command_uuid uuid not null,
  status_date timestamp without time zone not null default now(),
  status_type status_type_type not null,
  reason text null,
  foreign key (command_uuid) references command (command_uuid)
);

create function enqueue_command (command_uuid uuid, command_type text, command_data jsonb, command_auth jsonb)
returns void as $$
  declare next_status_t bigint;
  begin
    next_status_t := (select coalesce(max(status_t), 0) from status) + 1;
    insert into command (command_uuid, command_type, command_data, command_auth)
      values (command_uuid, command_type, command_data, command_auth);
    insert into queue (command_uuid) values (command_uuid);
    insert into status (status_t, command_uuid, status_type)
      values (next_status_t, command_uuid, 'queued');
    perform pg_notify('status', concat(next_status_t, ':queued:', command_uuid));
  end;
$$ language plpgsql;

create function fail_command (failed_command_uuid uuid, fail_reason text)
returns void as $$
  declare next_status_t bigint;
  begin
    next_status_t := (select coalesce(max(status_t), 0) from status) + 1;
    delete from queue where command_uuid = failed_command_uuid;
    insert into status (status_t, command_uuid, status_type, reason)
      values (next_status_t, failed_command_uuid, 'failed', fail_reason);
    perform pg_notify('status', concat(next_status_t, ':failed:', failed_command_uuid));
  end;
$$ language plpgsql;

create function abort_command (aborted_command_uuid uuid, abort_reason text)
returns void as $$
  declare next_status_t bigint;
  begin
    next_status_t := (select coalesce(max(status_t), 0) from status) + 1;
    delete from queue where command_uuid = aborted_command_uuid;
    insert into status (status_t, command_uuid, status_type, reason)
      values (next_status_t, aborted_command_uuid, 'aborted', abort_reason);
    perform pg_notify('status', concat(next_status_t, ':aborted:', aborted_command_uuid));
  end;
$$ language plpgsql;

create function succeed_command (this_command_uuid uuid, this_event_t bigint, this_event_data text)
returns void as $$
  declare next_status_t bigint;
  begin
    next_status_t := (select coalesce(max(status_t), 0) from status) + 1;
    if this_event_t != coalesce((select max(event_t) from event), 0) + 1 then
      raise exception 'invalid_event_t';
    end if;
    delete from queue where command_uuid = this_command_uuid;
    insert into status (status_t, command_uuid, status_type)
      values (next_status_t, this_command_uuid, 'succeeded');
    perform pg_notify('status', concat(next_status_t, ':succeeded:', this_command_uuid));
    insert into event (event_t, event_data, command_uuid)
      values (this_event_t, this_event_data::jsonb, this_command_uuid);
    perform pg_notify('event', this_event_t::text);
  end;
$$ language plpgsql;

