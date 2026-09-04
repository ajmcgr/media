-- Profile generation calls OpenAI. Keep the cost control server-side so a
-- browser cannot bypass it by repeatedly posting refresh=true.
create table if not exists public.profile_intelligence_generation_limits (
  user_id uuid primary key references auth.users(id) on delete cascade,
  window_started_on date not null default current_date,
  request_count integer not null default 0 check (request_count >= 0),
  updated_at timestamptz not null default now()
);

alter table public.profile_intelligence_generation_limits enable row level security;
revoke all on public.profile_intelligence_generation_limits from anon, authenticated;
grant all on public.profile_intelligence_generation_limits to service_role;

create or replace function public.consume_profile_intelligence_generation(
  _user uuid,
  _daily_limit integer
)
returns boolean
language plpgsql
security definer
set search_path = public
as $$
declare
  v_count integer;
begin
  if _user is null or coalesce(_daily_limit, 0) < 1 then
    raise exception 'invalid profile generation limit request';
  end if;

  insert into public.profile_intelligence_generation_limits (
    user_id, window_started_on, request_count, updated_at
  )
  values (_user, current_date, 1, now())
  on conflict (user_id) do update
    set window_started_on = current_date,
        request_count = case
          when public.profile_intelligence_generation_limits.window_started_on = current_date
            then public.profile_intelligence_generation_limits.request_count + 1
          else 1
        end,
        updated_at = now()
    where public.profile_intelligence_generation_limits.window_started_on <> current_date
       or public.profile_intelligence_generation_limits.request_count < _daily_limit
  returning request_count into v_count;

  return v_count is not null;
end;
$$;

revoke all on function public.consume_profile_intelligence_generation(uuid, integer) from public;
grant execute on function public.consume_profile_intelligence_generation(uuid, integer) to service_role;
