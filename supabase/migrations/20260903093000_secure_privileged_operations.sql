-- Secure privileged Edge Function operations and make top-up grants atomic.

-- A top-up may be confirmed by both the browser and Stripe's webhook. Insert
-- the idempotency row and grant credits in one transaction so only one wins.
create or replace function public.grant_topup_credits_once(
  _user uuid,
  _stripe_session_id text,
  _tokens bigint
)
returns boolean
language plpgsql
security definer
set search_path = public
as $$
declare
  transaction_id uuid;
begin
  if _user is null or coalesce(_tokens, 0) <= 0 or coalesce(trim(_stripe_session_id), '') = '' then
    raise exception 'invalid topup grant';
  end if;

  insert into public.topup_transactions (user_id, stripe_session_id, tokens)
  values (_user, _stripe_session_id, _tokens)
  on conflict (stripe_session_id) do nothing
  returning id into transaction_id;

  if transaction_id is null then
    return false;
  end if;

  insert into public.profiles (id, chat_credits)
  values (_user, _tokens)
  on conflict (id) do update
    set chat_credits = coalesce(public.profiles.chat_credits, 0) + excluded.chat_credits;

  return true;
end;
$$;

revoke all on function public.grant_topup_credits_once(uuid, text, bigint) from public;
grant execute on function public.grant_topup_credits_once(uuid, text, bigint) to service_role;

-- Workspace creation already creates the owner membership in a security-definer
-- trigger. All other joins must go through accept_team_invite, which validates
-- the invite email and the seat limit.
drop policy if exists "team_members_insert" on public.team_workspace_members;
create policy "team_members_insert" on public.team_workspace_members for insert to authenticated
  with check (
    public.is_team_owner(workspace_id, auth.uid())
    or public.is_team_admin(workspace_id, auth.uid())
  );

-- Keep the invite flow self-contained in the migration history.
drop policy if exists "team_invites_select_by_invitee_email" on public.team_workspace_invites;
create policy "team_invites_select_by_invitee_email"
on public.team_workspace_invites
for select
to authenticated
using (
  status = 'pending'
  and lower(email) = lower(coalesce(auth.jwt() ->> 'email', ''))
);

create or replace function public.accept_team_invite(_invite_id uuid)
returns public.team_workspace_members
language plpgsql
security definer
set search_path = public
as $$
declare
  v_invite public.team_workspace_invites;
  v_email text;
  v_member public.team_workspace_members;
  v_seat_limit integer;
  v_active_members integer;
begin
  if auth.uid() is null then raise exception 'not_authenticated'; end if;

  v_email := lower(coalesce(auth.jwt() ->> 'email', ''));
  select * into v_invite from public.team_workspace_invites where id = _invite_id;
  if not found then raise exception 'invite_not_found'; end if;
  if v_invite.status <> 'pending' then raise exception 'invite_%', v_invite.status; end if;
  if lower(v_invite.email) <> v_email then raise exception 'invite_email_mismatch'; end if;

  select seat_limit into v_seat_limit from public.team_workspaces where id = v_invite.workspace_id;
  select count(*) into v_active_members from public.team_workspace_members where workspace_id = v_invite.workspace_id;
  if v_active_members >= coalesce(v_seat_limit, 0) then raise exception 'seat_limit_reached'; end if;

  insert into public.team_workspace_members (workspace_id, user_id, role)
  values (v_invite.workspace_id, auth.uid(), v_invite.role)
  on conflict (workspace_id, user_id) do update set role = excluded.role
  returning * into v_member;

  update public.team_workspace_invites set status = 'accepted' where id = v_invite.id;
  return v_member;
end;
$$;

grant execute on function public.accept_team_invite(uuid) to authenticated;

-- Background jobs authenticate with the service-role key already stored in
-- Vault for the SEO cron. If the secret was never configured, leave schedules
-- untouched and emit a clear migration notice rather than silently disabling
-- the jobs.
create extension if not exists pg_cron with schema extensions;
create extension if not exists pg_net with schema extensions;
create extension if not exists supabase_vault with schema vault;

do $$
begin
  if exists (select 1 from vault.secrets where name = 'seo_cron_service_role_key') then
    if exists (select 1 from cron.job where jobname = 'blog-generate-3day') then
      perform cron.unschedule('blog-generate-3day');
    end if;
    perform cron.schedule(
      'blog-generate-3day',
      '0 9 */3 * *',
      $cron$
        select net.http_post(
          url := 'https://uavbphkhomblzkjfuaot.supabase.co/functions/v1/blog-generate',
          headers := jsonb_build_object(
            'Content-Type', 'application/json',
            'Authorization', 'Bearer ' || (
              select decrypted_secret
              from vault.decrypted_secrets
              where name = 'seo_cron_service_role_key'
            )
          ),
          body := jsonb_build_object('source', 'pg_cron'),
          timeout_milliseconds := 10000
        );
      $cron$
    );

    if exists (select 1 from cron.job where jobname = 'monitor-run-all-daily') then
      perform cron.unschedule('monitor-run-all-daily');
    end if;
    perform cron.schedule(
      'monitor-run-all-daily',
      '0 7 * * *',
      $cron$
        select net.http_post(
          url := 'https://uavbphkhomblzkjfuaot.supabase.co/functions/v1/monitor-run-all',
          headers := jsonb_build_object(
            'Content-Type', 'application/json',
            'Authorization', 'Bearer ' || (
              select decrypted_secret
              from vault.decrypted_secrets
              where name = 'seo_cron_service_role_key'
            )
          ),
          body := jsonb_build_object('source', 'cron')
        );
      $cron$
    );
  else
    raise warning 'seo_cron_service_role_key is not present in Vault; existing blog and monitor schedules were not changed.';
  end if;
end;
$$;
