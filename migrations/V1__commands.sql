create table command (
  command_uuid uuid primary key,
  command_type text not null,
  command_data jsonb not null,
  command_date timestamp without time zone not null default now()
);

create type command_queue_status as enum ('added', 'removed');

create table command_queue_tracker (
  queue_t bigint not null primary key,
  command_uuid uuid not null,
  status command_queue_status not null,
  foreign key (command_uuid) references command (command_uuid)
);

create table command_queue (
  command_uuid uuid not null primary key,
  queue_t bigint not null,
  scheduled_for timestamp without time zone not null default now(),
  foreign key (command_uuid) references command (command_uuid),
  foreign key (queue_t) references command_queue_tracker (queue_t),
  unique (queue_t)
);

create type command_outcome_type as enum ('succeeded', 'failed', 'aborted');

create table command_outcome (
  command_outcome_t bigint not null primary key,
  command_uuid uuid not null,
  command_outcome_date timestamp without time zone not null default now(),
  outcome command_outcome_type not null,
  reason text null,
  foreign key (command_uuid) references command (command_uuid),
  unique (command_uuid)
);

create table event (
  event_t bigint not null primary key,
  event_date timestamp without time zone not null default now(),
  event_data jsonb not null,
  command_uuid uuid not null,
  foreign key (command_uuid) references command (command_uuid),
  unique (command_uuid)
);

create function enqueue_command (command_uuid uuid, command_type text, command_data jsonb)
returns void as $$
  declare next_queue_t bigint;
  begin
    next_queue_t := (select coalesce(max(queue_t), 0) from command_queue_tracker) + 1;
    insert into command (command_uuid, command_type, command_data)
      values (command_uuid, command_type, command_data);
    insert into command_queue_tracker (queue_t, command_uuid, status)
      values (next_queue_t, command_uuid, 'added');
    insert into command_queue (queue_t, command_uuid)
      values (next_queue_t, command_uuid);
    perform pg_notify('command_stream', concat(next_queue_t, ':queued:', command_uuid));
  end;
$$ language plpgsql;

create function fail_command (failed_command_uuid uuid, failed_reason text)
returns void as $$
  declare next_queue_t bigint;
  begin
    insert into command_outcome (command_outcome_t, command_uuid, outcome, reason) 
      values (
        coalesce((select max(command_outcome_t) from command_outcome), 0) + 1, 
        failed_command_uuid,
        'failed',
        failed_reason
      );
    delete from command_queue T where T.command_uuid = failed_command_uuid;
    next_queue_t := (select coalesce(max(queue_t), 0) from command_queue_tracker) + 1;
    insert into command_queue_tracker (queue_t, command_uuid, status)
      values (next_queue_t, failed_command_uuid, 'removed');
    perform pg_notify('command_stream', concat(next_queue_t, ':failed:', failed_command_uuid));
  end;
$$ language plpgsql;

create function abort_command (command_uuid uuid, reason text)
returns void as $$
  declare next_queue_t bigint;
  begin
    insert into command_outcome (command_outcome_t, command_uuid, outcome, reason) 
      values (
        coalesce((select max(command_outcome_t) from command_outcome), 0) + 1, 
        command_uuid, 
        'failed',
        reason
      );
    delete from command_queue T where T.command_uuid = command_uuid;
    next_queue_t := (select coalesce(max(queue_t), 0) from command_queue_tracker) + 1;
    insert into command_queue_tracker (queue_t, command_uuid, status)
      values (next_queue_t, command_uuid, 'removed');
    perform pg_notify('command_stream', concat(next_queue_t, ':aborted:', command_uuid));
  end;
$$ language plpgsql;

create function succeed_command (command_uuid uuid, event_t bigint, event_data jsonb)
returns void as $$
  declare next_queue_t bigint;
  begin
    if event_t != coalesce((select max(event_t) from event), 0) + 1 then
      raise exception 'invalid event_t';
    end if;
    insert into command_outcome (command_outcome_t, command_uuid, outcome) 
      values (
        coalesce((select max(command_outcome_t) from command_outcome), 0) + 1, 
        command_uuid, 
        'succeeded'
      );
    delete from command_queue T where T.command_uuid = command_uuid;
    next_queue_t := (select coalesce(max(queue_t), 0) from command_queue_tracker) + 1;
    insert into command_queue_tracker (queue_t, command_uuid, status)
      values (next_queue_t, command_uuid, 'removed');
    perform pg_notify('command_stream', concat(next_queue_t, ':succeeded:', command_uuid));
    insert into event (event_t, event_data, command_uuid)
      values (event_t, event_data, command_uuid);
    perform pg_notify('event_stream', event_t);
  end;
$$ language plpgsql;

