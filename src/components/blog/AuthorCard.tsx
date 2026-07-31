import { AUTHOR } from "@/lib/blog/derive";

const AuthorCard = () => (
  <aside className="rounded-xl border border-border bg-card p-6 flex gap-4">
    <div
      className="h-11 w-11 shrink-0 rounded-full bg-primary/10 text-primary grid place-items-center text-sm font-medium"
      aria-hidden="true"
    >
      {AUTHOR.avatarInitials}
    </div>
    <div>
      <p className="text-sm font-medium text-foreground">{AUTHOR.name}</p>
      <p className="text-xs text-muted-foreground mb-2">{AUTHOR.role}</p>
      <p className="text-sm text-muted-foreground leading-relaxed">{AUTHOR.bio}</p>
    </div>
  </aside>
);

export default AuthorCard;
