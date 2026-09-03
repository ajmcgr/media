import { createClient } from "https://esm.sh/@supabase/supabase-js@2.45.0";

export type PrivilegedCaller =
  | { kind: "internal" }
  | { kind: "admin"; userId: string };

export type UserOrInternalCaller =
  | { kind: "internal" }
  | { kind: "user"; userId: string };

type AuthResult<T> = { caller: T } | { error: string; status: number };

function bearerToken(req: Request) {
  return (req.headers.get("Authorization") ?? "").replace(/^Bearer\s+/i, "").trim();
}

function env(name: string) {
  const value = Deno.env.get(name)?.trim();
  if (!value) throw new Error(`Missing ${name}`);
  return value;
}

async function authenticatedUser(token: string) {
  const url = env("SUPABASE_URL");
  const serviceKey = env("SUPABASE_SERVICE_ROLE_KEY");
  const admin = createClient(url, serviceKey);
  const { data, error } = await admin.auth.getUser(token);
  if (error || !data.user) return null;
  return { user: data.user, admin };
}

/**
 * Internal scheduled calls use the service-role bearer token. Browser callers
 * must be an explicit app administrator, not merely an authenticated user.
 */
export async function requireAdminOrInternal(req: Request): Promise<AuthResult<PrivilegedCaller>> {
  try {
    const token = bearerToken(req);
    if (!token) return { error: "missing_auth", status: 401 };

    const serviceKey = env("SUPABASE_SERVICE_ROLE_KEY");
    if (token === serviceKey) return { caller: { kind: "internal" } };

    const result = await authenticatedUser(token);
    if (!result) return { error: "invalid_auth", status: 401 };

    const { data, error } = await result.admin
      .from("user_roles")
      .select("role")
      .eq("user_id", result.user.id)
      .eq("role", "admin")
      .maybeSingle();
    if (error || !data) return { error: "forbidden", status: 403 };

    return { caller: { kind: "admin", userId: result.user.id } };
  } catch (error) {
    console.error("privileged authorization failed", error);
    return { error: "authorization_unavailable", status: 503 };
  }
}

/**
 * Used for user-owned work. Internal schedulers may process all records;
 * browser callers are later constrained to their authenticated user ID.
 */
export async function requireUserOrInternal(req: Request): Promise<AuthResult<UserOrInternalCaller>> {
  try {
    const token = bearerToken(req);
    if (!token) return { error: "missing_auth", status: 401 };

    const serviceKey = env("SUPABASE_SERVICE_ROLE_KEY");
    if (token === serviceKey) return { caller: { kind: "internal" } };

    const result = await authenticatedUser(token);
    if (!result) return { error: "invalid_auth", status: 401 };
    return { caller: { kind: "user", userId: result.user.id } };
  } catch (error) {
    console.error("user authorization failed", error);
    return { error: "authorization_unavailable", status: 503 };
  }
}
