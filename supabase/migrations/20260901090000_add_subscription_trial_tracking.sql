alter table public.profiles
  add column if not exists trial_used_at timestamptz;

comment on column public.profiles.trial_used_at is
  'Timestamp of the first Stripe subscription trial started by this user.';

update public.profiles p
set trial_used_at = coalesce(
  p.trial_used_at,
  (
    select min(s.created_at)
    from public.subscriptions s
    where s.user_id = p.id
  )
)
where p.trial_used_at is null
  and exists (
    select 1
    from public.subscriptions s
    where s.user_id = p.id
  );
