import { useState } from "react";
import { Check, Copy, Linkedin, Twitter, Link2 } from "lucide-react";
import { Button } from "@/components/ui/button";

const ShareBar = ({ url, title }: { url: string; title: string }) => {
  const [copied, setCopied] = useState(false);

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(url);
      setCopied(true);
      setTimeout(() => setCopied(false), 1800);
    } catch {
      /* clipboard unavailable */
    }
  };

  const enc = encodeURIComponent;

  return (
    <div className="flex items-center gap-2">
      <Button variant="outline" size="sm" className="rounded-md gap-2" onClick={copy} aria-label="Copy link to this article">
        {copied ? <Check className="h-3.5 w-3.5" /> : <Link2 className="h-3.5 w-3.5" />}
        {copied ? "Copied" : "Copy link"}
      </Button>
      <Button variant="outline" size="sm" className="rounded-md gap-2" asChild>
        <a
          href={`https://twitter.com/intent/tweet?text=${enc(title)}&url=${enc(url)}`}
          target="_blank"
          rel="noopener noreferrer"
          aria-label="Share on X"
        >
          <Twitter className="h-3.5 w-3.5" />
          Share
        </a>
      </Button>
      <Button variant="outline" size="sm" className="rounded-md gap-2" asChild>
        <a
          href={`https://www.linkedin.com/sharing/share-offsite/?url=${enc(url)}`}
          target="_blank"
          rel="noopener noreferrer"
          aria-label="Share on LinkedIn"
        >
          <Linkedin className="h-3.5 w-3.5" />
          Post
        </a>
      </Button>
      <Button variant="ghost" size="sm" className="rounded-md gap-2" asChild>
        <a href={`mailto:?subject=${enc(title)}&body=${enc(url)}`} aria-label="Share by email">
          <Copy className="h-3.5 w-3.5" />
          Email
        </a>
      </Button>
    </div>
  );
};

export default ShareBar;
