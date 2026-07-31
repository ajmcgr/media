-- Blog enhancements: view counts for Trending / Most read.
-- Additive only. The blog renders fine before this runs.
-- Run in the Supabase SQL editor (project ref: uavbphkhomblzkjfuaot).

create table if not exists public.blog_post_views (
  slug text primary key,
  views bigint not null default 0,
  updated_at timestamptz not null default now()
);

grant select on public.blog_post_views to anon, authenticated;
grant all on public.blog_post_views to service_role;

alter table public.blog_post_views enable row level security;

drop policy if exists "blog_post_views public read" on public.blog_post_views;
create policy "blog_post_views public read"
  on public.blog_post_views for select using (true);

-- Increment via a security-definer RPC so no client write policy is needed.
create or replace function public.increment_blog_view(p_slug text)
returns void
language plpgsql
security definer
set search_path = public
as $$
begin
  insert into public.blog_post_views (slug, views, updated_at)
  values (p_slug, 1, now())
  on conflict (slug) do update
    set views = public.blog_post_views.views + 1,
        updated_at = now();
end;
$$;

grant execute on function public.increment_blog_view(text) to anon, authenticated;

-- Optional: track edits so "Recently updated" is real rather than derived.
alter table public.blog_posts add column if not exists updated_at timestamptz not null default now();

create or replace function public.tg_blog_posts_touch()
returns trigger language plpgsql as $$
begin new.updated_at = now(); return new; end $$;

drop trigger if exists blog_posts_touch on public.blog_posts;
create trigger blog_posts_touch before update on public.blog_posts
  for each row execute function public.tg_blog_posts_touch();
