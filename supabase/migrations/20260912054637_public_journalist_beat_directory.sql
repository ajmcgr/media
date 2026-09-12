-- Public, aggregate-only directory inventory for indexable journalist beat pages.
-- The view respects the journalist table's RLS policies and never exposes emails.
create or replace view public.public_journalist_beats
with (security_invoker = true)
as
with normalized as (
  select
    lower(trim(category)) as beat_key,
    regexp_replace(lower(trim(category)), '[^a-z0-9]+', '-', 'g') as beat_slug,
    name,
    outlet,
    topics,
    titles
  from public.journalist
  where nullif(trim(coalesce(category, '')), '') is not null
    and char_length(trim(category)) between 2 and 60
)
select
  beat_key,
  beat_slug,
  count(*)::integer as total_contacts,
  count(*) filter (
    where nullif(trim(coalesce(name, '')), '') is not null
      and nullif(trim(coalesce(outlet, '')), '') is not null
      and (
        nullif(trim(coalesce(topics, '')), '') is not null
        or nullif(trim(coalesce(titles, '')), '') is not null
      )
  )::integer as qualified_contacts
from normalized
group by beat_key, beat_slug;

grant select on public.public_journalist_beats to anon, authenticated;
