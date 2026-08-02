import { Link } from "react-router-dom";
import { Clock } from "lucide-react";
import type { PostSummary } from "@/lib/blog/derive";
import { formatDate } from "@/lib/blog/derive";
import { blogImage } from "@/lib/blog/images";

type Props = {
  post: PostSummary;
  variant?: "default" | "compact" | "featured";
};

const PostCard = ({ post, variant = "default" }: Props) => {
  if (variant === "compact") {
    return (
      <Link to={`/blog/${post.slug}`} className="group flex gap-4 py-3">
        <div className="min-w-0">
          <p className="text-[11px] uppercase tracking-[0.14em] text-muted-foreground mb-1">
            {post.category.name}
          </p>
          <h3 className="text-sm font-medium leading-snug text-foreground group-hover:text-primary transition-colors line-clamp-2">
            {post.title}
          </h3>
          <p className="mt-1 text-xs text-muted-foreground">{post.readingMinutes} min read</p>
        </div>
      </Link>
    );
  }

  if (variant === "featured") {
    return (
      <Link to={`/blog/${post.slug}`} className="group grid gap-8 md:grid-cols-2 md:items-center">
        <div className="aspect-[16/10] overflow-hidden rounded-2xl bg-muted">
          <img
            src={blogImage(post.image, "hero")}
            alt={post.title}
            width={1200}
            height={750}
            fetchPriority="high"
            className="h-full w-full object-cover transition-transform duration-500 group-hover:scale-[1.02]"
          />
        </div>
        <div>
          <p className="text-xs uppercase tracking-[0.18em] text-muted-foreground mb-4">
            Featured · {post.category.name}
          </p>
          <h2 className="text-3xl md:text-4xl font-medium leading-tight tracking-tight text-foreground group-hover:text-primary transition-colors mb-4">
            {post.title}
          </h2>
          <p className="text-muted-foreground mb-5 line-clamp-3">{post.description}</p>
          <p className="text-sm text-muted-foreground">
            {formatDate(post.published)} · {post.readingMinutes} min read
          </p>
        </div>
      </Link>
    );
  }

  return (
    <Link to={`/blog/${post.slug}`} className="group block">
      <div className="aspect-[16/10] overflow-hidden rounded-xl bg-muted mb-5">
        <img
          src={blogImage(post.image, "card")}
          alt={post.title}
          loading="lazy"
          width={800}
          height={500}
          className="h-full w-full object-cover transition-transform duration-500 group-hover:scale-[1.02]"
        />
      </div>
      <p className="text-[11px] uppercase tracking-[0.14em] text-muted-foreground mb-2">
        {post.category.name}
      </p>
      <h3 className="text-xl font-medium leading-snug tracking-tight text-foreground group-hover:text-primary transition-colors mb-2">
        {post.title}
      </h3>
      <p className="text-sm text-muted-foreground line-clamp-2 mb-3">{post.description}</p>
      <p className="flex items-center gap-2 text-xs text-muted-foreground">
        {formatDate(post.published)}
        <span aria-hidden="true">·</span>
        <Clock className="h-3 w-3" aria-hidden="true" />
        {post.readingMinutes} min read
      </p>
    </Link>
  );
};

export default PostCard;
