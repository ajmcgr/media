-- Programmatic SEO landing pages (e.g. /discover/top-ai-journalists)
-- Run this in the Supabase SQL editor.

create table if not exists public.seo_pages (
  id uuid primary key default gen_random_uuid(),
  slug text not null unique,
  source text not null check (source in ('journalist','creator')),
  title text not null,
  h1 text not null,
  meta_description text not null,
  intro_html text not null default '',
  filters jsonb not null default '{}'::jsonb,
  -- supported filter keys (all optional):
  --  topics:string, category:string, country:string, outlet:string,
  --  ig_followers_min:number, youtube_subs_min:number, limit:number(default 50)
  faq jsonb not null default '[]'::jsonb,
  published boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists seo_pages_published_idx on public.seo_pages (published);
create index if not exists seo_pages_slug_idx on public.seo_pages (slug);

alter table public.seo_pages enable row level security;

drop policy if exists seo_pages_public_read on public.seo_pages;
create policy seo_pages_public_read on public.seo_pages
  for select using (published = true);

drop policy if exists seo_pages_auth_read on public.seo_pages;
create policy seo_pages_auth_read on public.seo_pages
  for select to authenticated using (true);

-- Writes are admin-only (see docs/sql/admin_roles.sql for user_roles + has_role).
drop policy if exists seo_pages_auth_write on public.seo_pages;
drop policy if exists seo_pages_admin_write on public.seo_pages;
create policy seo_pages_admin_write on public.seo_pages
  for all to authenticated
  using (public.has_role(auth.uid(), 'admin'))
  with check (public.has_role(auth.uid(), 'admin'));

create or replace function public.tg_seo_pages_touch()
returns trigger language plpgsql as $$
begin new.updated_at = now(); return new; end $$;

drop trigger if exists seo_pages_touch on public.seo_pages;
create trigger seo_pages_touch before update on public.seo_pages
  for each row execute function public.tg_seo_pages_touch();
