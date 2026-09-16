import DOMPurify from "dompurify";

/**
 * Sanitize untrusted email HTML before rendering it in the app.
 * Email bodies are fully attacker-controlled, so we allow-list only
 * inert formatting markup and strip scripts, event handlers, embeds
 * and javascript:/data: URLs.
 */
export function sanitizeEmailHtml(html: string | null | undefined): string {
  if (!html) return "";
  return DOMPurify.sanitize(html, {
    ALLOWED_TAGS: [
      "a", "b", "i", "em", "strong", "u", "s", "p", "br", "hr", "span", "div",
      "ul", "ol", "li", "blockquote", "pre", "code",
      "h1", "h2", "h3", "h4", "h5", "h6",
      "table", "thead", "tbody", "tfoot", "tr", "td", "th", "caption",
      "img", "figure", "figcaption", "small", "sub", "sup",
    ],
    ALLOWED_ATTR: ["href", "title", "alt", "src", "width", "height", "align", "colspan", "rowspan", "target", "rel"],
    ALLOWED_URI_REGEXP: /^(?:https?:|mailto:|tel:|cid:|#)/i,
    FORBID_TAGS: ["script", "style", "iframe", "object", "embed", "form", "input", "button", "link", "meta", "base", "svg", "math"],
    FORBID_ATTR: ["style", "srcset", "formaction", "background", "onerror", "onload"],
    ADD_ATTR: ["target"],
    KEEP_CONTENT: true,
  });
}
