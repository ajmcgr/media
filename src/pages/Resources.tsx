import { useState, useEffect } from "react";
import { updatePageSEO, addStructuredData } from "@/utils/seo";
import Layout from "@/components/Layout";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Link } from "react-router-dom";
import { 
  Search, 
  Filter,
  ArrowRight,
  Clock,
  BookOpen,
  TrendingUp,
  Users,
  FileText,
  Shield,
  Lightbulb,
  Star,
  Zap,
  RefreshCw,
  BarChart3
} from "lucide-react";

const Resources = () => {
  const [searchTerm, setSearchTerm] = useState("");
  const [selectedCategory, setSelectedCategory] = useState("All");

  const categories = ["All", "Pitching", "Releases", "Influencer", "Measurement", "Workflow", "Compliance", "Thought Leadership"];

  const allResources = [
    // Pitching Resources
    { 
      title: "How to Build a Media List That Actually Gets Replies", 
      description: "Define your audience, map beats, and create targeted lists that earn coverage.", 
      readingTime: "8 min read",
      slug: "build-a-media-list-that-gets-replies", 
      category: "Pitching",
      icon: Users,
      badge: "Popular" 
    },
    { 
      title: "The Art of Follow-Up: Converting No-Responses to Coverage", 
      description: "Strategic follow-up sequences that turn silence into story placements without being pushy.", 
      readingTime: "7 min read",
      slug: "follow-up-strategies-for-pr-coverage", 
      category: "Pitching",
      icon: RefreshCw,
      badge: "New" 
    },
    { 
      title: "Embargo Etiquette & Timing", 
      description: "When to pitch, who to brief, and how to set fair embargoes.", 
      readingTime: "5 min read",
      slug: "embargo-etiquette-and-timing", 
      category: "Pitching",
      icon: Clock,
      badge: null 
    },
    { 
      title: "Follow-Up Without Being Annoying", 
      description: "Timing, tone, and templates to follow up like a pro.", 
      readingTime: "4 min read",
      slug: "follow-up-without-being-annoying", 
      category: "Pitching",
      icon: FileText,
      badge: null 
    },
    { 
      title: "Data Storytelling for PR", 
      description: "Turn product or survey data into defensible, newsworthy angles.", 
      readingTime: "9 min read",
      slug: "data-storytelling-for-pr", 
      category: "Pitching",
      icon: TrendingUp,
      badge: null 
    },
    { 
      title: "Exclusive vs Wide Pitching: How to Choose", 
      description: "A decision framework for coverage quality and speed.", 
      readingTime: "6 min read",
      slug: "exclusive-vs-wide-pitching", 
      category: "Pitching",
      icon: BookOpen,
      badge: null 
    },

    // Releases Resources
    { 
      title: "Press Release Templates by Announcement Type", 
      description: "Ready-to-use structures for funding, product, partnership, and event releases.", 
      readingTime: "7 min read",
      slug: "press-release-templates-by-announcement-type", 
      category: "Releases",
      icon: FileText,
      badge: "Popular" 
    },
    { 
      title: "A 30-Day PR Plan for Product Launches", 
      description: "Day-by-day checklist from pre-brief to post-coverage.", 
      readingTime: "10 min read",
      slug: "30-day-pr-plan-for-product-launches", 
      category: "Releases",
      icon: BookOpen,
      badge: null 
    },
    { 
      title: "PR for Funding Announcements", 
      description: "Exclusives, syndication, and investor quotes done right.", 
      readingTime: "8 min read",
      slug: "pr-for-funding-announcements", 
      category: "Releases",
      icon: TrendingUp,
      badge: null 
    },
    { 
      title: "Build a Press Kit Journalists Actually Use", 
      description: "Files, formats, and structure that remove friction.", 
      readingTime: "5 min read",
      slug: "press-kit-that-journalists-use", 
      category: "Releases",
      icon: FileText,
      badge: null 
    },

    // Influencer Resources
    { 
      title: "Influencer Briefs That Drive Results", 
      description: "Clear deliverables, usage rights, and creative freedom.", 
      readingTime: "6 min read",
      slug: "influencer-briefs-that-drive-results", 
      category: "Influencer",
      icon: Users,
      badge: null 
    },
    { 
      title: "FTC/ASA Disclosure for Influencer Campaigns", 
      description: "Simple rules, clear captions, and platform nuances.", 
      readingTime: "4 min read",
      slug: "ftc-asa-disclosure-for-campaigns", 
      category: "Influencer",
      icon: Shield,
      badge: null 
    },
    { 
      title: "Using Reviews & Social Proof in PR", 
      description: "Turn G2/Capterra and UGC into media-worthy angles.", 
      readingTime: "5 min read",
      slug: "using-reviews-and-social-proof-in-pr", 
      category: "Influencer",
      icon: TrendingUp,
      badge: "Trending" 
    },

    // Measurement Resources
    { 
      title: "Social Media ROI: Proving PR Value in the Digital Age", 
      description: "Track engagement, conversions, and brand lift to demonstrate clear PR ROI.", 
      readingTime: "8 min read",
      slug: "social-media-roi-measurement-guide", 
      category: "Measurement",
      icon: BarChart3,
      badge: "New" 
    },
    { 
      title: "PR Attribution with UTMs & Post-Coverage Tracking", 
      description: "Set UTM standards and measure coverage effects properly.", 
      readingTime: "7 min read",
      slug: "pr-attribution-with-utms", 
      category: "Measurement",
      icon: BookOpen,
      badge: null 
    },
    { 
      title: "Digital PR for Link Building (Without Spam)", 
      description: "Earn high-quality links through stories, not schemes.", 
      readingTime: "8 min read",
      slug: "digital-pr-link-building", 
      category: "Measurement",
      icon: TrendingUp,
      badge: null 
    },

    // Workflow Resources
    { 
      title: "Founder Media Training: The Basics", 
      description: "Message maps, bridging, and quote-worthy soundbites.", 
      readingTime: "6 min read",
      slug: "founder-media-training-basics", 
      category: "Workflow",
      icon: Users,
      badge: null 
    },
    { 
      title: "Working with Freelance Journalists", 
      description: "Why freelancers matter and how to collaborate well.", 
      readingTime: "5 min read",
      slug: "working-with-freelance-journalists", 
      category: "Workflow",
      icon: Users,
      badge: null 
    },
    { 
      title: "Pitching Podcasts for Brand Story", 
      description: "Find, pitch, and prep for high-fit podcasts.", 
      readingTime: "7 min read",
      slug: "pitching-podcasts-for-brand-story", 
      category: "Workflow",
      icon: BookOpen,
      badge: null 
    },
    { 
      title: "Messaging & Positioning for PR Teams", 
      description: "Craft narratives editors—and customers—remember.", 
      readingTime: "8 min read",
      slug: "messaging-and-positioning-for-pr", 
      category: "Workflow",
      icon: Lightbulb,
      badge: null 
    },
    { 
      title: "International PR: Localize by Market", 
      description: "Angles, assets, and timing that respect each region.", 
      readingTime: "9 min read",
      slug: "international-pr-localization", 
      category: "Workflow",
      icon: BookOpen,
      badge: null 
    },

    // Compliance Resources
    { 
      title: "Crisis Communications Playbook", 
      description: "Holding statements, approvals, and rapid triage.", 
      readingTime: "10 min read",
      slug: "crisis-communications-playbook", 
      category: "Compliance",
      icon: Shield,
      badge: null 
    },
    { 
      title: "Seasonality, Calendars & Newsjacking (Safely)", 
      description: "Plan content around moments without being tone-deaf.", 
      readingTime: "6 min read",
      slug: "seasonality-calendar-and-newsjacking", 
      category: "Compliance",
      icon: Clock,
      badge: null 
    },

    // Thought Leadership Resources
    { 
      title: "Thought Leadership: From Idea to Op-Ed", 
      description: "Turn founder POVs into publishable bylines.", 
      readingTime: "8 min read",
      slug: "thought-leadership-to-op-ed", 
      category: "Thought Leadership",
      icon: Lightbulb,
      badge: null 
    }
  ];

  const filteredResources = allResources.filter(resource => {
    const matchesSearch = resource.title.toLowerCase().includes(searchTerm.toLowerCase()) ||
                         resource.description.toLowerCase().includes(searchTerm.toLowerCase());
    const matchesCategory = selectedCategory === "All" || resource.category === selectedCategory;
    return matchesSearch && matchesCategory;
  });

  const getBadgeColor = (badge: string | null) => {
    switch (badge) {
      case "Popular":
        return "bg-primary text-primary-foreground";
      case "New":
        return "bg-green-500 text-white";
      case "Trending":
        return "bg-orange-500 text-white";
      default:
        return "";
    }
  };

  useEffect(() => {
    updatePageSEO(
      "Free PR & Social Media Resources Hub | Media AI",
      "Expert guides and templates for PR professionals. Build media lists, craft pitches, measure coverage, and scale your PR strategy.",
      "PR resources, media list building, pitch templates, PR measurement, crisis communications, influencer marketing, press releases",
      "https://trymedia.ai/resources",
    );

    // Add structured data
    addStructuredData({
      "@context": "https://schema.org",
      "@type": "WebPage",
      "name": "PR & Social Media Resources Hub",
      "description": "Expert guides and templates for PR professionals",
      "url": window.location.href,
      "publisher": {
        "@type": "Organization",
        "name": "Media AI"
      }
    });
  }, []);

  return (
    <Layout>
      {/* Hero Section */}
      <section className="bg-hero-gradient py-16">
        <div className="container mx-auto px-4">
          <div className="max-w-3xl mx-auto text-center">
            <h1 className="text-4xl md:text-5xl font-bold text-white mb-4">
              Free PR & Social Media Resources
            </h1>
            <p className="text-xl text-white/90 mb-8">
              24 expert guides, templates, and playbooks to streamline your PR workflow. 
              Build media lists, craft pitches, and measure coverage impact.
            </p>
            <div className="flex items-center justify-center space-x-2 text-white/80">
              <Star className="h-5 w-5 text-yellow-300 fill-current" />
              <span>Always free • No signup required • Expert guides</span>
            </div>
          </div>
        </div>
      </section>

      {/* Resources Section */}
      <section className="py-16 bg-subtle-gradient">
        <div className="container mx-auto px-4">
          {/* Search and Filters */}
          <div className="max-w-4xl mx-auto mb-12">
            <div className="flex flex-col md:flex-row gap-4 items-center">
              <div className="relative flex-1">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-muted-foreground h-4 w-4" />
                <Input
                  placeholder="Search resources..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="pl-10 input-field"
                />
              </div>
              <div className="flex items-center space-x-2">
                <Filter className="h-4 w-4 text-muted-foreground" />
                <div className="flex flex-wrap gap-2">
                  {categories.map(category => (
                    <Button
                      key={category}
                      variant={selectedCategory === category ? "default" : "secondary"}
                      size="sm"
                      onClick={() => setSelectedCategory(category)}
                      className={selectedCategory === category ? "btn-primary" : "btn-secondary"}
                    >
                      {category}
                    </Button>
                  ))}
                </div>
              </div>
            </div>
          </div>

          {/* Resources Grid */}
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {filteredResources.map((resource, index) => {
              const IconComponent = resource.icon;
              const isExternalLink = resource.slug.startsWith('http');
              
              const cardContent = (
                <>
                  <div className="flex items-start justify-between mb-4">
                    <div className="p-3 bg-primary/10 rounded-xl">
                      <IconComponent className="h-6 w-6 text-primary" />
                    </div>
                    <div className="flex flex-col items-end space-y-2">
                      {resource.badge && (
                        <Badge className={getBadgeColor(resource.badge)}>
                          {resource.badge}
                        </Badge>
                      )}
                      <Badge variant="secondary" className="text-xs">
                        {resource.category}
                      </Badge>
                    </div>
                  </div>
                  
                  <h3 className="text-xl font-semibold text-foreground mb-2 group-hover:text-primary transition-colors">
                    {resource.title}
                  </h3>
                  
                  <p className="text-muted-foreground mb-4 leading-relaxed text-sm">
                    {resource.description}
                  </p>
                  
                  <div className="flex items-center justify-between mb-4">
                    <span className="text-sm font-medium text-primary">
                      Read guide
                    </span>
                    <ArrowRight className="h-4 w-4 text-muted-foreground group-hover:text-primary group-hover:translate-x-1 transition-all" />
                  </div>
                </>
              );

              return (
                <Card key={resource.slug} className="card-tool group cursor-pointer">
                  {isExternalLink ? (
                    <a href={resource.slug} target="_blank" rel="noopener noreferrer" className="block">
                      {cardContent}
                    </a>
                  ) : (
                    <Link to={`/resources/${resource.slug}`} className="block">
                      {cardContent}
                    </Link>
                  )}
                  
                  {/* CTA Section */}
                  <div className="mt-4 pt-4 border-t border-border">
                    <p className="text-xs text-muted-foreground mb-2">
                      Ready for the full experience?
                    </p>
                    <Button asChild size="sm" variant="outline" className="w-full text-xs">
                      <a 
                        href="https://trymedia.ai" 
                        target="_blank" 
                        rel="noopener noreferrer"
                      >
                        Try Media AI Platform
                      </a>
                    </Button>
                  </div>
                </Card>
              );
            })}
          </div>

          {filteredResources.length === 0 && (
            <div className="text-center py-12">
              <p className="text-muted-foreground text-lg">No resources found matching your criteria.</p>
              <Button 
                onClick={() => {
                  setSearchTerm("");
                  setSelectedCategory("All");
                }}
                variant="secondary"
                className="mt-4"
              >
                Clear filters
              </Button>
            </div>
          )}
        </div>
      </section>

      {/* CTA Section */}
      <section className="py-16 bg-white">
        <div className="container mx-auto px-4">
          <div className="max-w-3xl mx-auto text-center">
            <h2 className="text-3xl font-bold text-foreground mb-4">
              Ready to put these strategies into action?
            </h2>
            <p className="text-lg text-muted-foreground mb-8">
              These guides work even better with Media AI's database of 50,000+ journalists, 
              influencers, and podcasters.
            </p>
            <Button asChild className="btn-primary">
              <a 
                href="https://trymedia.ai" 
                target="_blank" 
                rel="noopener noreferrer"
              >
                <Zap className="mr-2 h-4 w-4" />
                Start 30-day trial
              </a>
            </Button>
          </div>
        </div>
      </section>
    </Layout>
  );
};

export default Resources;
