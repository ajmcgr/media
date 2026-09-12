import { useEffect, useMemo } from "react";
import { Link, useParams } from "react-router-dom";
import { ArrowRight, ExternalLink, Lock, Search, Users } from "lucide-react";
import Layout from "@/components/Layout";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { journalistBeatFromRecord } from "@/lib/journalistBeats";
import { useJournalistBeat, useJournalistBeats, usePublicJournalistsForBeat } from "@/hooks/useJournalistBeats";
import { setRobotsDirective, setStructuredData, updatePageSEO } from "@/utils/seo";
import { trackEvent } from "@/lib/analytics";

function profileSummary(contact: { titles: string | null; outlet: string | null; topics: string | null }) {
  const roleAndOutlet = [contact.titles, contact.outlet].filter(Boolean).join(" at ");
  return roleAndOutlet || contact.topics || "Journalist profile";
}

export default function JournalistBeatPage() {
  const { beat: slug } = useParams<{ beat: string }>();
  const { data: record, isLoading: beatLoading } = useJournalistBeat(slug);
  const { data: contacts, isLoading: contactsLoading } = usePublicJournalistsForBeat(record?.beat_key);
  const { data: allBeats } = useJournalistBeats();
  const beat = record ? journalistBeatFromRecord(record) : null;

  const related = useMemo(() => {
    if (!beat || !allBeats) return [];
    const order = [...beat.related, ...allBeats.map((item) => item.beat_slug)];
    const uniqueSlugs = [...new Set(order)].filter((candidate) => candidate !== beat.slug).slice(0, 4);
    return uniqueSlugs
      .map((candidate) => allBeats.find((item) => item.beat_slug === candidate))
      .filter(Boolean)
      .map((item) => journalistBeatFromRecord(item!));
  }, [allBeats, beat]);

  useEffect(() => {
    if (!beat || !record) return;
    const url = `https://trymedia.ai/journalists/${beat.slug}`;
    const title = `${beat.title} | Media AI`;
    const description = `${beat.description} Browse ${record.qualified_contacts.toLocaleString()} named contacts with outlets and coverage details.`;
    updatePageSEO(title, description, `${beat.name} journalists, ${beat.name} reporters, journalist directory`, url);
    setRobotsDirective("index,follow");
    setStructuredData("journalist-beat-breadcrumbs", {
      "@context": "https://schema.org",
      "@type": "BreadcrumbList",
      itemListElement: [
        { "@type": "ListItem", position: 1, name: "Home", item: "https://trymedia.ai/" },
        { "@type": "ListItem", position: 2, name: "Journalists by beat", item: "https://trymedia.ai/journalists" },
        { "@type": "ListItem", position: 3, name: beat.title, item: url },
      ],
    });
    setStructuredData("journalist-beat-list", {
      "@context": "https://schema.org",
      "@type": "ItemList",
      name: beat.title,
      description,
      numberOfItems: record.qualified_contacts,
      itemListElement: (contacts ?? []).slice(0, 25).map((contact, index) => ({
        "@type": "ListItem",
        position: index + 1,
        name: contact.name,
      })),
    });
    trackEvent("organic_landing_page_view", { page_type: "journalist_beat", beat: beat.slug, contacts: record.qualified_contacts });
  }, [beat, contacts, record]);

  useEffect(() => {
    if (!beatLoading && !record) setRobotsDirective("noindex,follow");
  }, [beatLoading, record]);

  if (beatLoading) {
    return <Layout><div className="container mx-auto max-w-5xl space-y-5 px-4 py-16"><Skeleton className="h-12 w-2/3" /><Skeleton className="h-28 w-full" /></div></Layout>;
  }

  if (!beat || !record) {
    return (
      <Layout>
        <div className="container mx-auto max-w-2xl px-4 py-24 text-center">
          <h1 className="text-3xl font-medium">Directory not available</h1>
          <p className="mt-3 text-muted-foreground">This beat does not yet meet Media AI’s public directory quality standard.</p>
          <Button asChild className="mt-6"><Link to="/journalists">Browse journalist beats</Link></Button>
        </div>
      </Layout>
    );
  }

  const visibleContacts = contacts?.filter((contact) => contact.name && contact.outlet) ?? [];
  const shown = Math.min(visibleContacts.length, 50);

  return (
    <Layout>
      <article>
        <header className="bg-hero-gradient py-14">
          <div className="container mx-auto max-w-5xl px-4">
            <nav aria-label="Breadcrumb" className="mb-4 text-sm text-white/80">
              <Link to="/journalists" className="hover:text-white">Journalists by beat</Link><span className="mx-2">/</span><span>{beat.name}</span>
            </nav>
            <div className="max-w-3xl">
              <p className="mb-3 text-sm font-medium text-white/80">Public journalist directory</p>
              <h1 className="text-4xl font-medium text-white md:text-5xl">{beat.title}</h1>
              <p className="mt-4 text-lg text-white/85">{beat.description}</p>
              <div className="mt-6 inline-flex items-center rounded-full bg-white/15 px-4 py-2 text-sm text-white"><Users className="mr-2 h-4 w-4" /> {record.qualified_contacts.toLocaleString()} qualified contacts in this beat</div>
            </div>
          </div>
        </header>

        <section className="bg-white py-10">
          <div className="container mx-auto grid max-w-5xl gap-6 px-4 md:grid-cols-[1.5fr_1fr]">
            <div>
              <h2 className="text-2xl font-medium">What this directory includes</h2>
              <p className="mt-3 text-muted-foreground">
                This public view shows named journalists in Media AI’s {beat.name} category with an outlet and coverage information. Browse the first {shown || "available"} profiles below; sign up to search a specific campaign angle and unlock verified contact details.
              </p>
            </div>
            <Card className="p-5">
              <p className="text-sm font-medium">Looking for a particular angle?</p>
              <p className="mt-1 text-sm text-muted-foreground">Search by topic, company, country, outlet, or campaign.</p>
              <Link
                to="/signup?next=/search"
                className="mt-4 inline-flex items-center text-sm font-medium text-primary"
                onClick={() => trackEvent("organic_landing_cta_clicked", { page_type: "journalist_beat", beat: beat.slug, cta: "search_beat" })}
              >
                Search this beat in Media AI <ArrowRight className="ml-1 h-4 w-4" />
              </Link>
            </Card>
          </div>
        </section>

        <section className="bg-subtle-gradient py-12">
          <div className="container mx-auto max-w-5xl px-4">
            <div className="mb-6 flex flex-col justify-between gap-2 sm:flex-row sm:items-end">
              <div><h2 className="text-2xl font-medium">Journalists in this directory</h2><p className="mt-1 text-sm text-muted-foreground">Outlets and coverage details are public. Email access is available in Media AI.</p></div>
              <span className="text-sm text-muted-foreground">Showing {shown} of {record.qualified_contacts.toLocaleString()}</span>
            </div>
            {contactsLoading ? (
              <div className="space-y-3">{Array.from({ length: 10 }).map((_, index) => <Skeleton key={index} className="h-24 w-full rounded-xl" />)}</div>
            ) : !visibleContacts.length ? (
              <Card className="p-8 text-center text-muted-foreground">This beat is being refreshed. Please try again soon.</Card>
            ) : (
              <ol className="space-y-3">
                {visibleContacts.map((contact, index) => (
                  <li key={contact.id}>
                    <Card className="flex items-start gap-4 p-4">
                      <span className="flex h-9 w-9 shrink-0 items-center justify-center rounded-full bg-primary/10 text-sm font-medium text-primary">{index + 1}</span>
                      <div className="min-w-0 flex-1">
                        <div className="flex flex-wrap items-center gap-2"><h3 className="font-medium text-foreground">{contact.name}</h3>{contact.country && <Badge variant="secondary">{contact.country}</Badge>}</div>
                        <p className="mt-1 text-sm text-muted-foreground">{profileSummary(contact)}</p>
                        {contact.topics && <p className="mt-1 line-clamp-2 text-xs text-muted-foreground">Beats: {contact.topics}</p>}
                        <div className="mt-2 flex flex-wrap gap-3 text-xs text-muted-foreground">
                          {contact.linkedin_url && <a href={contact.linkedin_url} target="_blank" rel="nofollow noopener noreferrer" className="inline-flex items-center hover:text-primary">LinkedIn <ExternalLink className="ml-1 h-3 w-3" /></a>}
                          {contact.xhandle && <a href={`https://x.com/${contact.xhandle.replace(/^@/, "")}`} target="_blank" rel="nofollow noopener noreferrer" className="hover:text-primary">@{contact.xhandle.replace(/^@/, "")}</a>}
                        </div>
                      </div>
                      <span className="hidden shrink-0 items-center text-xs text-muted-foreground sm:flex"><Lock className="mr-1 h-3 w-3" /> Email in Media AI</span>
                    </Card>
                  </li>
                ))}
              </ol>
            )}
          </div>
        </section>

        {related.length > 0 && <section className="bg-white py-12"><div className="container mx-auto max-w-5xl px-4"><h2 className="mb-5 text-2xl font-medium">Related journalist directories</h2><div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">{related.map((item) => <Link key={item.slug} to={`/journalists/${item.slug}`} className="group"><Card className="h-full p-4"><p className="font-medium group-hover:text-primary">{item.title}</p><p className="mt-2 line-clamp-2 text-sm text-muted-foreground">{item.description}</p><span className="mt-4 inline-flex items-center text-sm text-primary">Browse <ArrowRight className="ml-1 h-4 w-4" /></span></Card></Link>)}</div><div className="mt-7 flex flex-wrap gap-4 text-sm"><Link to="/guides/find-journalists-by-beat" className="text-primary hover:underline">How to find journalists by beat</Link><Link to="/tools/beat-outlet-matcher" className="text-primary hover:underline">Try the beat-to-outlet matcher</Link></div></div></section>}

        <section className="bg-subtle-gradient py-14"><div className="container mx-auto max-w-3xl px-4 text-center"><h2 className="text-2xl font-medium md:text-3xl">Build a focused {beat.name} media list</h2><p className="mt-3 text-muted-foreground">Search journalists in plain English, verify contact details, and save the people relevant to your next story.</p><Button asChild size="lg" className="btn-primary mt-6"><Link to="/signup?next=/search" onClick={() => trackEvent("organic_landing_cta_clicked", { page_type: "journalist_beat", beat: beat.slug, cta: "start_free" })}><Search className="mr-2 h-4 w-4" /> Start free</Link></Button></div></section>
      </article>
    </Layout>
  );
}
