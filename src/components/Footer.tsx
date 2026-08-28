import { Link } from "react-router-dom";
import { Linkedin } from "lucide-react";

const XIcon = ({ className }: { className?: string }) => (
  <svg viewBox="0 0 24 24" className={className} fill="currentColor" aria-hidden="true">
    <path d="M18.244 2H21.5l-7.5 8.57L23 22h-6.844l-5.36-6.99L4.6 22H1.34l8.02-9.16L1 2h7.02l4.84 6.39L18.244 2zm-1.2 18h1.86L7.04 4H5.06l11.984 16z" />
  </svg>
);

const RESOURCES_LINKS: ReadonlyArray<readonly [string, string]> = [
  ["Build a Media List", "/resources/build-a-media-list-that-gets-replies"],
  ["Pitching Framework", "/resources/exclusive-vs-wide-pitching"],
  ["Press Release Templates", "/resources/press-release-templates-by-announcement-type"],
  ["30 Day PR Plan", "/resources/30-day-pr-plan-for-product-launches"],
  ["PR For Funding", "/resources/pr-for-funding-announcements"],
  ["Build a Press Kit", "/resources/press-kit-that-journalists-use"],
  ["Influencer Briefs", "/resources/influencer-briefs-that-drive-results"],
  ["FTC Disclosures", "/resources/ftc-asa-disclosure-for-campaigns"],
  ["Social Proof in PR", "/resources/using-reviews-and-social-proof-in-pr"],
];

const COMPARE_LINKS: ReadonlyArray<readonly [string, string]> = [
  ["Media AI vs Muck Rack", "/compare/muck-rack"],
  ["Media AI vs Cision", "/compare/cision"],
  ["Media AI vs Meltwater", "/compare/meltwater"],
  ["Media AI vs GRIN", "/compare/grin"],
  ["Media AI vs HypeAuditor", "/compare/hypeauditor"],
  ["Media AI vs Later", "/compare/later"],
  ["Media AI vs Impact.com", "/compare/impact-com"],
];

const TOOLS_LINKS: ReadonlyArray<readonly [string, string]> = [
  ["Beat & Outlet Matcher", "/tools/beat-outlet-matcher"],
  ["Pitch Personalization Generator", "/tools/pitch-personalization-helper"],
  ["Subject Line Split-Tester", "/tools/subject-line-split-tester"],
  ["Pitch Fit Score Calculator", "/tools/pitch-fit-score-calculator"],
  ["Embargo & Timing Planner", "/tools/embargo-timing-planner"],
  ["List Segmenter", "/tools/list-segmenter-lite"],
  ["Outreach Sequence Generator", "/tools/outreach-sequence-generator"],
  ["Press Release Structure Generator", "/tools/press-release-structure-builder"],
  ["Quote Polisher for PR", "/tools/quote-polisher-pr"],
];

const FooterCol = ({ title, links }: { title: string; links: readonly (readonly [string, string])[] }) => (
  <div>
    <h4 className="font-medium mb-4" style={{ fontFamily: "var(--font-heading)" }}>{title}</h4>
    <ul className="space-y-2.5 text-muted-foreground">
      {links.map(([label, href]) => (
        <li key={label}>
          {href.startsWith("http") || href.startsWith("mailto:") ? (
            <a href={href} className="hover:text-foreground" target={href.startsWith("http") ? "_blank" : undefined} rel="noopener noreferrer">{label}</a>
          ) : (
            <Link to={href} className="hover:text-foreground">{label}</Link>
          )}
        </li>
      ))}
    </ul>
  </div>
);

const Footer = () => {
  return (
    <footer className="px-6 py-16 mt-12">
      <div className="max-w-6xl mx-auto grid md:grid-cols-6 gap-8 text-sm">
        <FooterCol title="Company" links={[
          ["About", "/about"],
          ["AI Info", "/ai-info"],
          ["Blog", "/blog"],
          ["Media Kit", "/media-kit"],
          ["Community", "https://chat.whatsapp.com/KKjLvfjPY2ND11cexE0Tae?mode=gi_t"],
        ]} />
        <FooterCol title="Support" links={[
          ["Support", "mailto:support@trymedia.ai"],
          ["Privacy Policy", "/privacy"],
          ["Terms of Service", "/terms"],
        ]} />
        <div>
          <h4 className="font-medium mb-4" style={{ fontFamily: "var(--font-heading)" }}>Resources</h4>
          <ul className="space-y-2.5 text-muted-foreground">
            {RESOURCES_LINKS.map(([label, href]) => (
              <li key={label}>
                <Link to={href} className="hover:text-foreground">{label}</Link>
              </li>
            ))}
            <li><Link to="/resources" className="hover:text-foreground font-medium">View All Resources →</Link></li>
          </ul>
        </div>
        <div>
          <h4 className="font-medium mb-4" style={{ fontFamily: "var(--font-heading)" }}>Free Tools</h4>
          <ul className="space-y-2.5 text-muted-foreground">
            {TOOLS_LINKS.map(([label, href]) => (
              <li key={label}>
                <Link to={href} className="hover:text-foreground">{label}</Link>
              </li>
            ))}
            <li><Link to="/tools" className="hover:text-foreground font-medium">View All Tools →</Link></li>
          </ul>
        </div>
        <div>
          <h4 className="font-medium mb-4" style={{ fontFamily: "var(--font-heading)" }}>Compare</h4>
          <ul className="space-y-2.5 text-muted-foreground">
            {COMPARE_LINKS.map(([label, href]) => (
              <li key={label}>
                <Link to={href} className="hover:text-foreground">{label}</Link>
              </li>
            ))}
            <li><Link to="/compare" className="hover:text-foreground font-medium">View All →</Link></li>
          </ul>
        </div>
        <div>
          <h4 className="font-medium mb-4" style={{ fontFamily: "var(--font-heading)" }}>Connect</h4>
          <div className="flex items-center gap-3">
            <a href="http://x.com/trymediaai" target="_blank" rel="noopener noreferrer" className="h-9 w-9 rounded-full border border-border flex items-center justify-center hover:bg-secondary"><XIcon className="h-4 w-4" /></a>
            <a href="https://www.linkedin.com/company/trymediaai" target="_blank" rel="noopener noreferrer" className="h-9 w-9 rounded-full border border-border flex items-center justify-center hover:bg-secondary"><Linkedin className="h-4 w-4" /></a>
          </div>
        </div>
      </div>
      <p className="text-xs text-muted-foreground text-center mt-12">
          Copyright © {new Date().getFullYear()} Works App, Inc. Built with 🫶🏻 by{" "}
          <a href="http://x.com/alexmacgregor__/" target="_blank" rel="noopener noreferrer" className="hover:text-foreground">Alex</a>.
      </p>
    </footer>
  );
};

export default Footer;
