create table command (
  command_uuid uuid primary key,
  command_type text not null,
  command_data jsonb not null,
  command_date timestamp without time zone not null default now()
);

create table command_queue (
  command_uuid uuid not null primary key,
  scheduled_for timestamp without time zone not null default now(),
  foreign key (command_uuid) references command (command_uuid)
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
  begin
    insert into command (command_uuid, command_type, command_data)
      values (command_uuid, command_type, command_data);
    insert into command_queue (command_uuid)
      values (command_uuid);
    perform pgnotify('command_stream', concat('queued:', command_uuid));
  end;
$$ language plpgsql;

create function fail_command (command_uuid uuid, reason text)
returns void as $$
  begin
    insert into command_outcome (command_outcome_t, command_uuid, outcome, reason) 
      values (
        coalesce((select max(command_outcome_t) from command_outcome), 1), 
        command_uuid, 
        'failed',
        reason
      );
    delete from command_queue T where T.command_uuid = command_uuid;
    perform pgnotify('command_stream', concat('failed:', command_uuid));
  end;
$$ language plpgsql;

create function abort_command (command_uuid uuid, reason text)
returns void as $$
  begin
    insert into command_outcome (command_outcome_t, command_uuid, outcome, reason) 
      values (
        coalesce((select max(command_outcome_t) from command_outcome), 1), 
        command_uuid, 
        'failed',
        reason
      );
    delete from command_queue T where T.command_uuid = command_uuid;
    perform pgnotify('command_stream', concat('aborted:', command_uuid));
  end;
$$ language plpgsql;

create function succeed_command (command_uuid uuid, event_t bigint, event_data jsonb)
returns void as $$
  begin
    if event_t != coalesce((select max(event_t) from event), 0) + 1 then
      raise exception 'invalid event_t';
    end if;
    insert into command_outcome (command_outcome_t, command_uuid, outcome) 
      values (
        coalesce((select max(command_outcome_t) from command_outcome), 1), 
        command_uuid, 
        'succeeded'
      );
    delete from command_queue T where T.command_uuid = command_uuid;
    perform pgnotify('command_stream', concat('succeeded:', command_uuid));
    insert into event (event_t, event_data, command_uuid)
      values (event_t, event_data, command_uuid);
    perform pgnotify('event_stream', event_t);
  end;
$$ language plpgsql;

