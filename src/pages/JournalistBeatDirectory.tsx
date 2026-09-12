import { useEffect } from "react";
import { Link } from "react-router-dom";
import { ArrowRight, Search, Users } from "lucide-react";
import Layout from "@/components/Layout";
import { Card } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { journalistBeatFromRecord } from "@/lib/journalistBeats";
import { useJournalistBeats } from "@/hooks/useJournalistBeats";
import { setRobotsDirective, updatePageSEO } from "@/utils/seo";
import { trackEvent } from "@/lib/analytics";

export default function JournalistBeatDirectory() {
  const { data: beats, isLoading } = useJournalistBeats();

  useEffect(() => {
    updatePageSEO(
      "Journalists by Beat | Media AI",
      "Browse public journalist directories by beat, including technology, fashion, travel, automotive, crypto and more. See outlets and coverage before searching Media AI.",
      "journalists by beat, journalist directory, media contacts, reporters by topic",
      "https://trymedia.ai/journalists",
    );
    setRobotsDirective("index,follow");
    trackEvent("organic_landing_page_view", { page_type: "journalist_beat_directory" });
  }, []);

  return (
    <Layout>
      <section className="bg-hero-gradient py-16">
        <div className="container mx-auto max-w-4xl px-4 text-center">
          <p className="mb-3 text-sm font-medium text-white/80">Media AI directory</p>
          <h1 className="mb-4 text-4xl font-medium text-white md:text-5xl">Find journalists by beat</h1>
          <p className="mx-auto max-w-2xl text-lg text-white/85">
            Browse public directories of named journalists by coverage area, then search Media AI for the right people for your story.
          </p>
        </div>
      </section>

      <section className="bg-subtle-gradient py-14">
        <div className="container mx-auto max-w-6xl px-4">
          <div className="mb-8 flex flex-col justify-between gap-3 sm:flex-row sm:items-end">
            <div>
              <h2 className="text-2xl font-medium">Browse coverage beats</h2>
              <p className="mt-2 text-muted-foreground">Only beats with at least 50 named journalists, outlets and coverage details are included.</p>
            </div>
            {beats && <p className="text-sm text-muted-foreground">{beats.length} directories</p>}
          </div>

          {isLoading ? (
            <div className="grid gap-5 sm:grid-cols-2 lg:grid-cols-3">
              {Array.from({ length: 9 }).map((_, index) => <Skeleton key={index} className="h-44 rounded-xl" />)}
            </div>
          ) : !beats?.length ? (
            <Card className="p-8 text-center text-muted-foreground">The public directory is being refreshed. Please check back shortly.</Card>
          ) : (
            <div className="grid gap-5 sm:grid-cols-2 lg:grid-cols-3">
              {beats.map((record) => {
                const beat = journalistBeatFromRecord(record);
                return (
                  <Link
                    key={beat.slug}
                    to={`/journalists/${beat.slug}`}
                    className="group block"
                    onClick={() => trackEvent("organic_directory_page_clicked", { beat: beat.slug })}
                  >
                    <Card className="card-tool h-full">
                      <div className="mb-4 flex items-start justify-between gap-4">
                        <div className="rounded-xl bg-primary/10 p-3"><Users className="h-6 w-6 text-primary" /></div>
                        <span className="text-sm text-muted-foreground">{record.qualified_contacts.toLocaleString()} contacts</span>
                      </div>
                      <h3 className="text-lg font-medium text-foreground transition-colors group-hover:text-primary">{beat.title}</h3>
                      <p className="mt-2 line-clamp-2 text-sm text-muted-foreground">{beat.description}</p>
                      <div className="mt-5 inline-flex items-center text-sm font-medium text-primary">
                        Browse directory <ArrowRight className="ml-1 h-4 w-4 transition-transform group-hover:translate-x-1" />
                      </div>
                    </Card>
                  </Link>
                );
              })}
            </div>
          )}

          <Card className="mt-12 flex flex-col items-start justify-between gap-5 p-6 md:flex-row md:items-center">
            <div>
              <h2 className="text-xl font-medium">Need a list for a specific story?</h2>
              <p className="mt-1 text-muted-foreground">Use plain English to find relevant journalists, verify contacts and save a focused list.</p>
            </div>
            <Link
              to="/signup?next=/search"
              className="btn-primary inline-flex h-10 items-center justify-center rounded-md px-4 text-sm font-medium"
              onClick={() => trackEvent("organic_landing_cta_clicked", { page_type: "journalist_beat_directory", cta: "start_free" })}
            >
              <Search className="mr-2 h-4 w-4" /> Search Media AI
            </Link>
          </Card>
        </div>
      </section>
    </Layout>
  );
}
