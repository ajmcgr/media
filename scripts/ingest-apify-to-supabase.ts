import { createClient, type SupabaseClient } from "@supabase/supabase-js";

type TableName = "journalist" | "creators";
type ConflictStrategy =
    | "email"
    | "xhandle"
    | "ig_handle"
    | "youtube_url"
    | "rec_id"
    | "name_outlet"
    | "name_website";

type BaseNormalizedRecord = {
    source: string;
    source_url?: string;
};

type JournalistRecord = BaseNormalizedRecord & {
    table: "journalist";
    name?: string;
    email?: string;
    outlet?: string;
    xhandle?: string;
    website?: string;
    location?: string;
    country?: string;
};

type CreatorRecord = BaseNormalizedRecord & {
    table: "creators";
    name?: string;
    email?: string;
    ig_handle?: string;
    youtube_url?: string;
    rec_id?: string;
    website?: string;
    category?: string;
};

type NormalizedRecord = JournalistRecord | CreatorRecord;

type RuntimeConfig = {
    apifyToken: string;
    datasetIds: string[];
    taskIds: string[];
    supabaseUrl: string;
    supabaseServiceRoleKey: string;
    maxRecordsPerRun: number;
    batchSize: number;
    dryRun: boolean;
};

type DatasetPullStats = {
    datasetId: string;
    pulled: number;
};

type RunStats = {
    datasets: DatasetPullStats[];
    rawPulled: number;
    normalized: number;
    malformed: number;
    duplicatesSkipped: number;
    skippedMissingIgHandle: number;
    categorySetNull: number;
    journalistAttempted: number;
    creatorAttempted: number;
    rowsWritten: number;
    upsertedGroups: number;
    fallbackChecked: number;
    errors: number;
};

type ExistingRow = Record<string, unknown>;
type TableColumnMap = Record<TableName, Set<string>>;

const DEFAULT_MAX_RECORDS_PER_RUN = 1000;
const DEFAULT_BATCH_SIZE = 250;
const SAMPLE_RECORDS_LIMIT = 5;
const APIFY_PAGE_LIMIT = 250;
const NETWORK_RETRY_ATTEMPTS = 3;
const NETWORK_RETRY_DELAY_MS = 1000;

const stats: RunStats = {
    datasets: [],
    rawPulled: 0,
    normalized: 0,
    malformed: 0,
    duplicatesSkipped: 0,
    skippedMissingIgHandle: 0,
    categorySetNull: 0,
    journalistAttempted: 0,
    creatorAttempted: 0,
    rowsWritten: 0,
    upsertedGroups: 0,
    fallbackChecked: 0,
    errors: 0,
};

function log(message: string, extra?: Record<string, unknown>): void {
    const timestamp = new Date().toISOString();
    if (extra) {
        console.log(`[${timestamp}] ${message}`, extra);
        return;
    }
    console.log(`[${timestamp}] ${message}`);
}

function logRecoverableError(message: string, extra?: Record<string, unknown>): void {
    stats.errors += 1;
    log(message, extra);
}

function readRequiredEnv(name: string): string {
    const value = process.env[name]?.trim();
    if (!value) {
        throw new Error(`Missing required env var: ${name}`);
    }
    return value;
}

function parseIntegerEnv(name: string, fallback: number): number {
    const value = process.env[name];
    if (!value) {
        return fallback;
    }

    const parsed = Number.parseInt(value, 10);
    if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error(`Invalid positive integer for ${name}: ${value}`);
    }
    return parsed;
}

function parseBooleanEnv(name: string, fallback: boolean): boolean {
    const value = process.env[name];
    if (!value) {
        return fallback;
    }

    return ["1", "true", "yes", "on"].includes(value.trim().toLowerCase());
}

function getRuntimeConfig(): RuntimeConfig {
    const datasetIds = readRequiredEnv("APIFY_DATASET_IDS")
        .split(",")
        .map((id) => id.trim())
        .filter(Boolean);
    const taskIds = (process.env.APIFY_TASK_IDS ?? "")
        .split(",")
        .map((id) => id.trim())
        .filter(Boolean);

    if (datasetIds.length === 0 && taskIds.length === 0) {
        throw new Error("Provide at least one APIFY_DATASET_IDS or APIFY_TASK_IDS value");
    }

    return {
        apifyToken: readRequiredEnv("APIFY_TOKEN"),
        datasetIds,
        taskIds,
        supabaseUrl: readRequiredEnv("SUPABASE_URL"),
        supabaseServiceRoleKey: readRequiredEnv("SUPABASE_SERVICE_ROLE_KEY"),
        maxRecordsPerRun: parseIntegerEnv(
            "MAX_RECORDS_PER_RUN",
            DEFAULT_MAX_RECORDS_PER_RUN,
        ),
        batchSize: parseIntegerEnv("BATCH_SIZE", DEFAULT_BATCH_SIZE),
        dryRun: parseBooleanEnv("DRY_RUN", true),
    };
}

function chunk<T>(items: T[], size: number): T[][] {
    const chunks: T[][] = [];
    for (let index = 0; index < items.length; index += size) {
        chunks.push(items.slice(index, index + size));
    }
    return chunks;
}

function asString(value: unknown): string | undefined {
    if (typeof value === "string") {
        const trimmed = value.trim();
        return trimmed || undefined;
    }

    if (typeof value === "number" || typeof value === "boolean") {
        return String(value);
    }

    return undefined;
}

function extractFirstString(value: unknown): string | undefined {
    const direct = asString(value);
    if (direct) {
        return direct;
    }

    if (Array.isArray(value)) {
        for (const item of value) {
            const nested = extractFirstString(item);
            if (nested) {
                return nested;
            }
        }
        return undefined;
    }

    if (value && typeof value === "object") {
        const record = value as Record<string, unknown>;
        return (
            asString(record.url) ??
            asString(record.href) ??
            asString(record.link) ??
            asString(record.value)
        );
    }

    return undefined;
}

function coalesceString(record: Record<string, unknown>, keys: string[]): string | undefined {
    for (const key of keys) {
        const value = extractFirstString(record[key]);
        if (value) {
            return value;
        }
    }
    return undefined;
}

function getNestedValue(
    record: Record<string, unknown>,
    path: string[],
): unknown {
    let current: unknown = record;
    for (const key of path) {
        if (!current || typeof current !== "object" || Array.isArray(current)) {
            return undefined;
        }
        current = (current as Record<string, unknown>)[key];
    }
    return current;
}

function normalizeName(value?: string): string | undefined {
    if (!value) {
        return undefined;
    }
    return value.trim().replace(/\s+/g, " ").toLowerCase();
}

function normalizeHandle(value?: string): string | undefined {
    if (!value) {
        return undefined;
    }
    return value.replace(/^@+/, "").trim().toLowerCase() || undefined;
}

function normalizeCategorySlug(value?: string): string | undefined {
    if (!value) {
        return undefined;
    }

    const normalized = value
        .trim()
        .toLowerCase()
        .replace(/&/g, " and ")
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/^-+|-+$/g, "");

    return normalized || undefined;
}

function pushCandidate(candidates: string[], value?: string): void {
    if (!value) {
        return;
    }

    const trimmed = value.trim();
    if (!trimmed) {
        return;
    }

    candidates.push(trimmed);
}

function inferCreatorCategoryCandidates(rawCategory?: string): string[] {
    if (!rawCategory) {
        return [];
    }

    const candidates: string[] = [];
    const parts = rawCategory
        .split(/[|,/]+/)
        .map((part) => part.trim())
        .filter(Boolean);
    const normalizedParts = parts.map((part) => part.toLowerCase());

    pushCandidate(candidates, rawCategory);
    for (const part of parts) {
        pushCandidate(candidates, part);
    }

    const whole = rawCategory.toLowerCase();
    const partsText = normalizedParts.join(" ");

    const includesAny = (...needles: string[]): boolean =>
        needles.some((needle) => whole.includes(needle) || partsText.includes(needle));

    if (includesAny("travel", "tour", "adventure")) {
        pushCandidate(candidates, "travel");
    }
    if (includesAny("food", "foodie", "restaurant", "chef", "critic", "cooking")) {
        pushCandidate(candidates, "food");
    }
    if (
        includesAny(
            "fitness",
            "fit",
            "trainer",
            "athlete",
            "gym",
            "workout",
            "weight loss",
            "reel creator",
        )
    ) {
        pushCandidate(candidates, "fitness");
    }
    if (includesAny("health", "wellness", "medical")) {
        pushCandidate(candidates, "health");
    }
    if (includesAny("shopping", "product/service", "product service", "retail")) {
        pushCandidate(candidates, "Shopping");
    }
    if (includesAny("beauty", "cosmetic", "makeup", "skincare", "skin care")) {
        pushCandidate(candidates, "beauty-cosmetics");
    }
    if (includesAny("jewel", "jewell", "watch")) {
        pushCandidate(candidates, "jewellry-watches");
    }
    if (includesAny("tv", "film", "movie", "cinema")) {
        pushCandidate(candidates, "tv-film");
    }
    if (includesAny("photo", "photography")) {
        pushCandidate(candidates, "photography");
    }
    if (includesAny("design")) {
        pushCandidate(candidates, "design");
    }
    if (includesAny("sport")) {
        pushCandidate(candidates, "sports");
    }
    if (includesAny("children", "kids")) {
        pushCandidate(candidates, "Children");
    }
    if (includesAny("baby")) {
        pushCandidate(candidates, "baby");
    }
    if (includesAny("business", "entrepreneur", "economy")) {
        pushCandidate(candidates, "business");
    }
    if (includesAny("gaming", "game")) {
        pushCandidate(candidates, "gaming");
    }
    if (includesAny("electronic", "tech", "gadget")) {
        pushCandidate(candidates, "electronics");
    }
    if (includesAny("pet", "animal")) {
        pushCandidate(candidates, "pets");
    }

    return candidates;
}

function buildCreatorCategoryLookup(validCategories: string[]): Map<string, string> {
    const lookup = new Map<string, string>();

    for (const category of validCategories) {
        const direct = category.trim().toLowerCase();
        const slug = normalizeCategorySlug(category);

        lookup.set(direct, category);
        if (slug) {
            lookup.set(slug, category);
        }
    }

    return lookup;
}

function mapCreatorCategory(
    rawCategory: string | undefined,
    lookup: Map<string, string>,
): string | undefined {
    if (!rawCategory) {
        return undefined;
    }

    for (const candidate of inferCreatorCategoryCandidates(rawCategory)) {
        const direct = candidate.trim().toLowerCase();
        const slug = normalizeCategorySlug(candidate);
        const mapped = lookup.get(direct) ?? (slug ? lookup.get(slug) : undefined);

        if (mapped) {
            return mapped;
        }
    }

    return undefined;
}

function normalizeWebsite(value?: string): string | undefined {
    if (!value) {
        return undefined;
    }

    let next = value.trim().toLowerCase();
    next = next.replace(/^https?:\/\//, "");
    next = next.replace(/^www\./, "");
    next = next.replace(/\/+$/, "");
    return next || undefined;
}

function normalizeEmail(value?: string): string | undefined {
    return value?.trim().toLowerCase() || undefined;
}

function isCreatorLike(record: Record<string, unknown>): boolean {
    return Boolean(
        coalesceString(record, [
            "ig_handle",
            "instagram_handle",
            "instagram",
            "username",
            "youtube_url",
            "youtube",
            "channel_url",
            "rec_id",
            "id",
            "fbid",
            "creator_category",
            "niche",
            "businessCategoryName",
            "searchTerm",
        ]),
    );
}

function buildSourceUrl(record: Record<string, unknown>): string | undefined {
    return coalesceString(record, [
        "source_url",
        "url",
        "profile_url",
        "website_url",
        "linkedin_url",
        "page_url",
        "twitterUrl",
    ]);
}

function isTwitterJournalistLike(record: Record<string, unknown>): boolean {
    const searchTerm = coalesceString(record, [
        "searchTerm",
        "search_term",
        "query",
    ]);
    const nestedAuthorName = extractFirstString(getNestedValue(record, ["author", "name"]));
    const nestedAuthorHandle = extractFirstString(
        getNestedValue(record, ["author", "userName"]),
    );
    const tweetText = coalesceString(record, ["text", "fullText", "tweetText", "tweet"]);
    const tweetUrl =
        coalesceString(record, ["twitterUrl", "url", "tweetUrl"]) ??
        extractFirstString(getNestedValue(record, ["author", "url"]));

    return Boolean(
        searchTerm?.toLowerCase().includes("journalist") &&
            (nestedAuthorName || nestedAuthorHandle) &&
            (tweetText || tweetUrl),
    );
}

function normalizeTwitterJournalist(
    record: Record<string, unknown>,
): JournalistRecord | undefined {
    const name =
        extractFirstString(getNestedValue(record, ["author", "name"])) ??
        coalesceString(record, ["authorName", "name"]);
    const xhandle = normalizeHandle(
        extractFirstString(getNestedValue(record, ["author", "userName"])) ??
            coalesceString(record, ["userName", "username", "screen_name", "handle"]),
    );
    const website = normalizeWebsite(
        extractFirstString(getNestedValue(record, ["author", "url"])) ??
            (xhandle ? `https://x.com/${xhandle}` : undefined),
    );
    const location =
        extractFirstString(getNestedValue(record, ["author", "location"])) ??
        coalesceString(record, ["location"]);
    const sourceUrl =
        coalesceString(record, ["twitterUrl", "url", "tweetUrl"]) ??
        buildSourceUrl(record);

    if (!name && !xhandle && !website) {
        return undefined;
    }

    return {
        table: "journalist",
        name,
        xhandle,
        website,
        location,
        source: "twitter",
        source_url: sourceUrl,
    };
}

function normalizeJournalist(
    record: Record<string, unknown>,
): JournalistRecord | undefined {
    const name = coalesceString(record, ["name", "full_name", "journalist_name"]);
    const email = normalizeEmail(coalesceString(record, ["email", "contact_email"]));
    const outlet = coalesceString(record, ["outlet", "publication", "company"]);
    const xhandle = normalizeHandle(
        coalesceString(record, ["xhandle", "twitter_handle", "x_handle", "handle"]),
    );
    const website = normalizeWebsite(coalesceString(record, ["website", "domain", "site"]));
    const location = coalesceString(record, ["location", "city", "region"]);
    const country = coalesceString(record, ["country", "country_name"]);

    if (!name && !email && !outlet && !xhandle && !website) {
        return undefined;
    }

    return {
        table: "journalist",
        name,
        email,
        outlet,
        xhandle,
        website,
        location,
        country,
        source: coalesceString(record, ["source", "platform"]) ?? "apify",
        source_url: buildSourceUrl(record),
    };
}

function normalizeCreator(
    record: Record<string, unknown>,
): CreatorRecord | undefined {
    const name = coalesceString(record, [
        "name",
        "full_name",
        "fullName",
        "creator_name",
        "channel_name",
    ]);
    const email = normalizeEmail(coalesceString(record, ["email", "contact_email"]));
    const igHandle = normalizeHandle(
        coalesceString(record, ["ig_handle", "instagram_handle", "instagram", "username"]),
    );
    const youtubeUrl = normalizeWebsite(
        coalesceString(record, ["youtube_url", "youtube", "channel_url"]),
    );
    const recId = coalesceString(record, ["rec_id", "record_id", "creator_id", "id", "fbid"]);
    const website = normalizeWebsite(
        coalesceString(record, ["website", "domain", "site", "externalUrls"]),
    );
    const category = coalesceString(record, [
        "category",
        "creator_category",
        "niche",
        "businessCategoryName",
        "searchTerm",
    ]);

    if (!name && !email && !igHandle && !youtubeUrl && !recId && !website) {
        return undefined;
    }

    return {
        table: "creators",
        name,
        email,
        ig_handle: igHandle,
        youtube_url: youtubeUrl,
        rec_id: recId,
        website,
        category,
        source: coalesceString(record, ["source", "platform"]) ?? "apify",
        source_url: buildSourceUrl(record),
    };
}

function normalizeRecord(
    item: unknown,
    datasetId: string,
): NormalizedRecord | undefined {
    if (!item || typeof item !== "object" || Array.isArray(item)) {
        stats.malformed += 1;
        return undefined;
    }

    const record = item as Record<string, unknown>;
    const normalized = isTwitterJournalistLike(record)
        ? normalizeTwitterJournalist(record)
        : isCreatorLike(record)
          ? normalizeCreator(record)
          : normalizeJournalist(record);

    if (!normalized) {
        stats.malformed += 1;
        return undefined;
    }

    return normalized;
}

function getConflictStrategy(record: NormalizedRecord): ConflictStrategy {
    if (record.email) {
        return "email";
    }

    if (record.table === "journalist" && record.xhandle) {
        return "xhandle";
    }

    if (record.table === "creators" && record.ig_handle) {
        return "ig_handle";
    }

    if (record.table === "creators" && record.youtube_url) {
        return "youtube_url";
    }

    if (record.table === "creators" && record.rec_id) {
        return "rec_id";
    }

    if (record.name && "outlet" in record && record.outlet) {
        return "name_outlet";
    }

    return "name_website";
}

function buildLocalDedupKey(record: NormalizedRecord): string | undefined {
    const strategy = getConflictStrategy(record);
    switch (strategy) {
        case "email":
            return `email:${record.email}`;
        case "xhandle":
            return record.table === "journalist" && record.xhandle
                ? `xhandle:${record.xhandle}`
                : undefined;
        case "ig_handle":
            return record.table === "creators" && record.ig_handle
                ? `ig:${record.ig_handle}`
                : undefined;
        case "youtube_url":
            return record.table === "creators" && record.youtube_url
                ? `yt:${record.youtube_url}`
                : undefined;
        case "rec_id":
            return record.table === "creators" && record.rec_id
                ? `rec:${record.rec_id}`
                : undefined;
        case "name_outlet": {
            const outlet = "outlet" in record ? normalizeName(record.outlet) : undefined;
            const name = normalizeName(record.name);
            return name && outlet ? `name_outlet:${name}:${outlet}` : undefined;
        }
        case "name_website": {
            const website = normalizeWebsite(record.website);
            const name = normalizeName(record.name);
            return name && website ? `name_website:${name}:${website}` : undefined;
        }
    }
}

function dedupeLocally(records: NormalizedRecord[]): NormalizedRecord[] {
    const seen = new Set<string>();
    const deduped: NormalizedRecord[] = [];

    for (const record of records) {
        const dedupeKey = buildLocalDedupKey(record);
        if (dedupeKey && seen.has(dedupeKey)) {
            stats.duplicatesSkipped += 1;
            continue;
        }

        if (dedupeKey) {
            seen.add(dedupeKey);
        }
        deduped.push(record);
    }

    return deduped;
}

async function fetchDatasetItems(
    config: RuntimeConfig,
    datasetId: string,
    remainingBudget: number,
): Promise<unknown[]> {
    const items: unknown[] = [];
    let offset = 0;
    const limitPerPage = Math.min(APIFY_PAGE_LIMIT, remainingBudget);

    while (items.length < remainingBudget) {
        const pageLimit = Math.min(limitPerPage, remainingBudget - items.length);
        const url = new URL(`https://api.apify.com/v2/datasets/${datasetId}/items`);
        url.searchParams.set("token", config.apifyToken);
        url.searchParams.set("format", "json");
        url.searchParams.set("clean", "true");
        url.searchParams.set("offset", String(offset));
        url.searchParams.set("limit", String(pageLimit));

        const response = await withRetry(
            () =>
                fetch(url, {
                    method: "GET",
                    headers: {
                        Accept: "application/json",
                    },
                    signal: AbortSignal.timeout(30_000),
                }),
            `fetch dataset ${datasetId}`,
        );

        if (!response.ok) {
            throw new Error(
                `Apify dataset fetch failed for ${datasetId} with ${response.status} ${response.statusText}`,
            );
        }

        const page = (await response.json()) as unknown;
        if (!Array.isArray(page) || page.length === 0) {
            break;
        }

        items.push(...page);
        offset += page.length;

        if (page.length < pageLimit) {
            break;
        }
    }

    stats.datasets.push({ datasetId, pulled: items.length });
    return items;
}

async function fetchLatestTaskDatasetId(
    config: RuntimeConfig,
    taskId: string,
): Promise<string | undefined> {
    const url = new URL(`https://api.apify.com/v2/actor-tasks/${taskId}/runs`);
    url.searchParams.set("token", config.apifyToken);
    url.searchParams.set("status", "SUCCEEDED");
    url.searchParams.set("desc", "1");
    url.searchParams.set("limit", "1");

    const response = await withRetry(
        () =>
            fetch(url, {
                method: "GET",
                headers: {
                    Accept: "application/json",
                },
                signal: AbortSignal.timeout(30_000),
            }),
        `fetch task runs ${taskId}`,
    );

    if (!response.ok) {
        throw new Error(
            `Apify task run fetch failed for ${taskId} with ${response.status} ${response.statusText}`,
        );
    }

    const payload = (await response.json()) as {
        data?: { items?: Array<Record<string, unknown>> };
    };
    const latestRun = payload.data?.items?.[0];
    const datasetId = asString(latestRun?.defaultDatasetId);

    log("Resolved latest task dataset", {
        taskId,
        datasetId: datasetId ?? null,
        runId: asString(latestRun?.id) ?? null,
    });

    return datasetId;
}

async function fetchAllRecords(config: RuntimeConfig): Promise<NormalizedRecord[]> {
    const normalizedRecords: NormalizedRecord[] = [];
    const seenDatasetIds = new Set<string>();

    for (const datasetId of config.datasetIds) {
        const remainingBudget = config.maxRecordsPerRun - stats.rawPulled;
        if (remainingBudget <= 0) {
            break;
        }
        if (seenDatasetIds.has(datasetId)) {
            continue;
        }
        seenDatasetIds.add(datasetId);

        log("Fetching dataset", { datasetId, remainingBudget });
        const rawItems = await fetchDatasetItems(config, datasetId, remainingBudget);
        stats.rawPulled += rawItems.length;

        for (const item of rawItems) {
            const normalized = normalizeRecord(item, datasetId);
            if (normalized) {
                normalizedRecords.push(normalized);
            }
        }
    }

    for (const taskId of config.taskIds) {
        const remainingBudget = config.maxRecordsPerRun - stats.rawPulled;
        if (remainingBudget <= 0) {
            break;
        }

        const datasetId = await fetchLatestTaskDatasetId(config, taskId);
        if (!datasetId || seenDatasetIds.has(datasetId)) {
            continue;
        }
        seenDatasetIds.add(datasetId);

        log("Fetching latest task dataset", {
            taskId,
            datasetId,
            remainingBudget,
        });
        const rawItems = await fetchDatasetItems(config, datasetId, remainingBudget);
        stats.rawPulled += rawItems.length;

        for (const item of rawItems) {
            const normalized = normalizeRecord(item, datasetId);
            if (normalized) {
                normalizedRecords.push(normalized);
            }
        }
    }

    stats.normalized = normalizedRecords.length;
    return dedupeLocally(normalizedRecords);
}

function buildSamples(records: NormalizedRecord[]): Array<Record<string, unknown>> {
    return records.slice(0, SAMPLE_RECORDS_LIMIT).map((record) => ({ ...record }));
}

function summarize(records: NormalizedRecord[]): void {
    const journalistRows = records.filter((record) => record.table === "journalist").length;
    const creatorRows = records.length - journalistRows;

    stats.journalistAttempted = journalistRows;
    stats.creatorAttempted = creatorRows;

    log("Run summary", {
        datasets: stats.datasets,
        rawPulled: stats.rawPulled,
        normalized: stats.normalized,
        malformed: stats.malformed,
        duplicatesSkipped: stats.duplicatesSkipped,
        skippedMissingIgHandle: stats.skippedMissingIgHandle,
        categorySetNull: stats.categorySetNull,
        journalistRows,
        creatorRows,
        errors: stats.errors,
    });
}

type GroupedRecords = Record<ConflictStrategy, NormalizedRecord[]>;

function emptyGroupedRecords(): GroupedRecords {
    return {
        email: [],
        xhandle: [],
        ig_handle: [],
        youtube_url: [],
        rec_id: [],
        name_outlet: [],
        name_website: [],
    };
}

function groupRecords(records: NormalizedRecord[]): GroupedRecords {
    const grouped = emptyGroupedRecords();
    for (const record of records) {
        if (record.table === "creators") {
            if (!record.ig_handle) {
                stats.skippedMissingIgHandle += 1;
                continue;
            }

            grouped.ig_handle.push(record);
            continue;
        }

        grouped[getConflictStrategy(record)].push(record);
    }
    return grouped;
}

async function loadCreatorCategoryValues(
    supabase: SupabaseClient,
): Promise<string[]> {
    const { data, error } = await withRetry(
        () => supabase.from("creator_category").select("name"),
        "supabase select creator_category",
    );

    if (error) {
        throw new Error(`Could not load creator categories: ${error.message}`);
    }

    return (data ?? [])
        .map((row) => asString((row as Record<string, unknown>).name))
        .filter((value): value is string => Boolean(value));
}

function applyCreatorCategoryMapping(
    records: NormalizedRecord[],
    validCategories: string[],
): NormalizedRecord[] {
    const lookup = buildCreatorCategoryLookup(validCategories);

    return records.map((record) => {
        if (record.table !== "creators") {
            return record;
        }

        const mappedCategory = mapCreatorCategory(record.category, lookup);

        if (record.category) {
            if (!mappedCategory) {
                stats.categorySetNull += 1;
            }

            log("Creator category mapping", {
                rawCategory: record.category,
                mappedCategory: mappedCategory ?? null,
            });
        }

        return {
            ...record,
            category: mappedCategory,
        };
    });
}

async function run(): Promise<void> {
    const startedAt = Date.now();
    const config = getRuntimeConfig();
    const supabase = createClient(
        config.supabaseUrl,
        config.supabaseServiceRoleKey,
        {
            auth: {
                autoRefreshToken: false,
                persistSession: false,
            },
        },
    );

    log("Starting ingestion worker", {
        datasetIds: config.datasetIds,
        taskIds: config.taskIds,
        maxRecordsPerRun: config.maxRecordsPerRun,
        batchSize: config.batchSize,
        dryRun: config.dryRun,
    });

    const validCreatorCategories = await loadCreatorCategoryValues(supabase);
    log("Loaded valid creator categories", {
        count: validCreatorCategories.length,
        values: validCreatorCategories,
    });

    const records = applyCreatorCategoryMapping(
        await fetchAllRecords(config),
        validCreatorCategories,
    );
    summarize(records);

    if (config.dryRun) {
        log("Dry run enabled. No writes will be sent to Supabase.", {
            sampleRecords: buildSamples(records),
        });
        log("Dry run completed", {
            runtimeSeconds: Number(((Date.now() - startedAt) / 1000).toFixed(2)),
        });
        return;
    }

    const grouped = groupRecords(records);
    const columnMap = await loadTableColumnMap(supabase);
    const journalistGroups: Array<[ConflictStrategy, JournalistRecord[]]> = Object.entries(
        grouped,
    ).map(([strategy, group]) => [
        strategy as ConflictStrategy,
        group.filter((record): record is JournalistRecord => record.table === "journalist"),
    ]);
    const creatorGroups: Array<[ConflictStrategy, CreatorRecord[]]> = Object.entries(
        grouped,
    ).map(([strategy, group]) => [
        strategy as ConflictStrategy,
        group.filter((record): record is CreatorRecord => record.table === "creators"),
    ]);

    await processGroups(
        supabase,
        "journalist",
        journalistGroups,
        config.batchSize,
        columnMap.journalist,
    );

    await processGroups(
        supabase,
        "creators",
        creatorGroups,
        config.batchSize,
        columnMap.creators,
    );

    log("Live ingestion completed", {
        rowsWritten: stats.rowsWritten,
        upsertedGroups: stats.upsertedGroups,
        fallbackChecked: stats.fallbackChecked,
        skippedMissingIgHandle: stats.skippedMissingIgHandle,
        categorySetNull: stats.categorySetNull,
        errors: stats.errors,
        runtimeSeconds: Number(((Date.now() - startedAt) / 1000).toFixed(2)),
    });
}

async function processGroups<T extends JournalistRecord | CreatorRecord>(
    supabase: SupabaseClient,
    table: TableName,
    groups: Array<[ConflictStrategy, T[]]>,
    batchSize: number,
    allowedColumns: Set<string>,
): Promise<void> {
    for (const [strategy, records] of groups) {
        if (records.length === 0) {
            continue;
        }

        for (const batch of chunk(records, batchSize)) {
            if (strategy === "name_outlet" || strategy === "name_website") {
                await fallbackInsertBatch(supabase, table, strategy, batch, allowedColumns);
                continue;
            }

            await upsertBatch(supabase, table, strategy, batch, allowedColumns);
        }
    }
}

async function upsertBatch<T extends JournalistRecord | CreatorRecord>(
    supabase: SupabaseClient,
    table: TableName,
    strategy: Exclude<ConflictStrategy, "name_outlet" | "name_website">,
    records: T[],
    allowedColumns: Set<string>,
): Promise<void> {
    const payload = records
        .map((record) => sanitizeForTable(record, allowedColumns))
        .filter((record) => Object.keys(record).length > 0);

    if (payload.length === 0) {
        log("Skipped upsert batch because no safe columns were available", {
            table,
            strategy,
            attempted: records.length,
        });
        return;
    }

    const { error } = await withRetry(
        () =>
            supabase.from(table).upsert(payload, {
                onConflict: strategy,
                ignoreDuplicates: false,
            }),
        `supabase upsert ${table}.${strategy}`,
    );

    if (error) {
        if (
            table === "journalist" &&
            strategy === "xhandle" &&
            error.message.includes("no unique or exclusion constraint")
        ) {
            log("xhandle upsert unsupported, falling back to lookup+insert", {
                table,
                attempted: payload.length,
            });
            await fallbackInsertByColumn(
                supabase,
                table,
                "xhandle",
                payload,
                allowedColumns,
            );
            return;
        }

        if (payload.length === 1) {
            if (isCriticalSupabaseError(error.message)) {
                throw new Error(`Supabase upsert failed for ${table}.${strategy}: ${error.message}`);
            }

            logRecoverableError("Skipped bad record after single-row upsert failure", {
                table,
                strategy,
                error: error.message,
                record: payload[0],
            });
            return;
        }

        log("Batch upsert failed, retrying rows individually", {
            table,
            strategy,
            attempted: payload.length,
            error: error.message,
        });

        for (const row of payload) {
            await upsertBatch(
                supabase,
                table,
                strategy,
                [row as T],
                new Set(Object.keys(row)),
            );
        }
        return;
    }

    stats.rowsWritten += payload.length;
    stats.upsertedGroups += 1;
    log("Upserted batch", {
        table,
        strategy,
        attempted: payload.length,
    });
}

async function fallbackInsertBatch<T extends JournalistRecord | CreatorRecord>(
    supabase: SupabaseClient,
    table: TableName,
    strategy: Extract<ConflictStrategy, "name_outlet" | "name_website">,
    records: T[],
    allowedColumns: Set<string>,
): Promise<void> {
    stats.fallbackChecked += records.length;
    const existing = await loadFallbackCandidates(supabase, table, strategy, records);
    const existingKeys = new Set(existing.map((row) => existingFallbackKey(row, strategy)).filter(Boolean));

    const insertable = records.filter((record) => {
        const key = recordFallbackKey(record, strategy);
        return key ? !existingKeys.has(key) : true;
    });

    if (insertable.length === 0) {
        log("Skipped fallback batch because all rows already exist", {
            table,
            strategy,
            attempted: records.length,
        });
        return;
    }

    const payload = insertable
        .map((record) => sanitizeForTable(record, allowedColumns))
        .filter((record) => Object.keys(record).length > 0);

    if (payload.length === 0) {
        log("Skipped fallback batch because no safe columns were available", {
            table,
            strategy,
            attempted: records.length,
        });
        return;
    }

    const { error } = await withRetry(
        () => supabase.from(table).insert(payload),
        `supabase insert ${table}.${strategy}`,
    );
    if (error) {
        if (payload.length === 1) {
            if (isCriticalSupabaseError(error.message)) {
                throw new Error(`Supabase fallback insert failed for ${table}.${strategy}: ${error.message}`);
            }

            logRecoverableError("Skipped bad record after single-row fallback insert failure", {
                table,
                strategy,
                error: error.message,
                record: payload[0],
            });
            return;
        }

        log("Fallback insert batch failed, retrying rows individually", {
            table,
            strategy,
            attempted: payload.length,
            error: error.message,
        });

        for (const row of payload) {
            await fallbackInsertBatch(
                supabase,
                table,
                strategy,
                [row as T],
                new Set(Object.keys(row)),
            );
        }
        return;
    }

    stats.rowsWritten += payload.length;
    log("Inserted fallback batch", {
        table,
        strategy,
        attempted: records.length,
        inserted: insertable.length,
        skippedExisting: records.length - insertable.length,
    });
}

async function fallbackInsertByColumn(
    supabase: SupabaseClient,
    table: TableName,
    column: "xhandle",
    records: Record<string, unknown>[],
    allowedColumns: Set<string>,
): Promise<void> {
    const values = Array.from(
        new Set(records.map((record) => asString(record[column])).filter((value): value is string => Boolean(value))),
    );

    if (values.length === 0) {
        return;
    }

    const existing = await loadExistingByColumn(supabase, table, column, values);
    const existingValues = new Set(
        existing
            .map((row) => asString(row[column]))
            .filter((value): value is string => Boolean(value)),
    );

    const insertable = records.filter((record) => {
        const value = asString(record[column]);
        return value ? !existingValues.has(value) : false;
    });

    if (insertable.length === 0) {
        log("Skipped fallback insert by column because all rows already exist", {
            table,
            column,
            attempted: records.length,
        });
        return;
    }

    const payload = insertable
        .map((record) => sanitizeForTable(record, allowedColumns))
        .filter((record) => Object.keys(record).length > 0);

    if (payload.length === 0) {
        return;
    }

    const { error } = await withRetry(
        () => supabase.from(table).insert(payload),
        `supabase insert ${table}.${column}`,
    );

    if (error) {
        if (payload.length === 1) {
            if (isCriticalSupabaseError(error.message)) {
                throw new Error(`Supabase fallback insert failed for ${table}.${column}: ${error.message}`);
            }

            logRecoverableError("Skipped bad record after single-row xhandle fallback insert failure", {
                table,
                column,
                error: error.message,
                record: payload[0],
            });
            return;
        }

        log("Fallback xhandle insert batch failed, retrying rows individually", {
            table,
            column,
            attempted: payload.length,
            error: error.message,
        });

        for (const row of payload) {
            await fallbackInsertByColumn(
                supabase,
                table,
                column,
                [row],
                new Set(Object.keys(row)),
            );
        }
        return;
    }

    stats.rowsWritten += payload.length;
    log("Inserted fallback batch by column", {
        table,
        column,
        attempted: records.length,
        inserted: payload.length,
        skippedExisting: records.length - payload.length,
    });
}

async function loadExistingByColumn(
    supabase: SupabaseClient,
    table: TableName,
    column: "xhandle",
    values: string[],
): Promise<ExistingRow[]> {
    const rows: ExistingRow[] = [];

    for (const batch of chunk(values, DEFAULT_BATCH_SIZE)) {
        const { data, error } = await supabase
            .from(table)
            .select(column)
            .in(column, batch);

        if (error) {
            throw new Error(`Supabase lookup failed for ${table}.${column}: ${error.message}`);
        }

        if (Array.isArray(data)) {
            rows.push(...data);
        }
    }

    return rows;
}

async function loadFallbackCandidates<T extends JournalistRecord | CreatorRecord>(
    supabase: SupabaseClient,
    table: TableName,
    strategy: "name_outlet" | "name_website",
    records: T[],
): Promise<ExistingRow[]> {
    const nameValues = Array.from(
        new Set(records.map((record) => record.name).filter((value): value is string => Boolean(value))),
    );

    if (nameValues.length === 0) {
        return [];
    }

    const rows: ExistingRow[] = [];
    const columns = strategy === "name_outlet" ? "name,outlet" : "name,website";
    for (const names of chunk(nameValues, DEFAULT_BATCH_SIZE)) {
        const { data, error } = await supabase
            .from(table)
            .select(columns)
            .in("name", names);

        if (error) {
            throw new Error(`Supabase candidate lookup failed for ${table}: ${error.message}`);
        }

        if (Array.isArray(data)) {
            rows.push(...data);
        }
    }

    return rows;
}

function recordFallbackKey(
    record: JournalistRecord | CreatorRecord,
    strategy: "name_outlet" | "name_website",
): string | undefined {
    const normalizedName = normalizeName(record.name);
    if (!normalizedName) {
        return undefined;
    }

    if (strategy === "name_outlet" && "outlet" in record) {
        const outlet = normalizeName(record.outlet);
        return outlet ? `${normalizedName}::${outlet}` : undefined;
    }

    const website = normalizeWebsite(record.website);
    return website ? `${normalizedName}::${website}` : undefined;
}

function existingFallbackKey(
    row: ExistingRow,
    strategy: "name_outlet" | "name_website",
): string | undefined {
    const normalizedName = normalizeName(asString(row.name));
    if (!normalizedName) {
        return undefined;
    }

    if (strategy === "name_outlet") {
        const outlet = normalizeName(asString(row.outlet));
        return outlet ? `${normalizedName}::${outlet}` : undefined;
    }

    const website = normalizeWebsite(asString(row.website));
    return website ? `${normalizedName}::${website}` : undefined;
}

function stripUndefined<T extends Record<string, unknown>>(record: T): Record<string, unknown> {
    return Object.fromEntries(
        Object.entries(record).filter(([, value]) => value !== undefined),
    );
}

function sanitizeForTable<T extends Record<string, unknown>>(
    record: T,
    allowedColumns: Set<string>,
): Record<string, unknown> {
    const stripped = stripUndefined(record);
    return Object.fromEntries(
        Object.entries(stripped).filter(([key]) => key !== "table" && allowedColumns.has(key)),
    );
}

async function loadTableColumnMap(supabase: SupabaseClient): Promise<TableColumnMap> {
    return {
        journalist: await loadTableColumns(supabase, "journalist", [
            "name",
            "email",
            "outlet",
            "xhandle",
            "website",
            "location",
            "country",
            "source",
            "source_url",
        ]),
        creators: await loadTableColumns(supabase, "creators", [
            "name",
            "email",
            "ig_handle",
            "youtube_url",
            "rec_id",
            "website",
            "category",
            "source",
            "source_url",
        ]),
    };
}

async function loadTableColumns(
    supabase: SupabaseClient,
    table: TableName,
    fallbackColumns: string[],
): Promise<Set<string>> {
    const { data, error } = await supabase.from(table).select("*").limit(1);

    if (error) {
        log("Could not introspect table columns, falling back to default column list", {
            table,
            message: error.message,
        });
        return new Set(fallbackColumns);
    }

    if (Array.isArray(data) && data.length > 0) {
        return new Set(Object.keys(data[0] ?? {}));
    }

    return new Set(fallbackColumns);
}

function isCriticalSupabaseError(message: string): boolean {
    const normalized = message.toLowerCase();
    return (
        normalized.includes("jwt") ||
        normalized.includes("permission") ||
        normalized.includes("network") ||
        normalized.includes("failed to fetch") ||
        normalized.includes("service role") ||
        normalized.includes("auth")
    );
}

async function withRetry<T>(
    operation: () => PromiseLike<T>,
    label: string,
    attempts = NETWORK_RETRY_ATTEMPTS,
): Promise<T> {
    let lastError: unknown;

    for (let attempt = 1; attempt <= attempts; attempt += 1) {
        try {
            return await operation();
        } catch (error) {
            lastError = error;
            if (attempt >= attempts || !isRetryableError(error)) {
                break;
            }

            log("Transient error, retrying operation", {
                label,
                attempt,
                attempts,
                message: error instanceof Error ? error.message : String(error),
            });
            await delay(NETWORK_RETRY_DELAY_MS * attempt);
        }
    }

    throw new Error(
        `${label} failed after ${attempts} attempt(s): ${
            lastError instanceof Error ? lastError.message : String(lastError)
        }`,
    );
}

function isRetryableError(error: unknown): boolean {
    const message = error instanceof Error ? error.message.toLowerCase() : String(error).toLowerCase();
    return (
        message.includes("timeout") ||
        message.includes("timed out") ||
        message.includes("econnreset") ||
        message.includes("enotfound") ||
        message.includes("fetch failed") ||
        message.includes("network") ||
        message.includes("429") ||
        message.includes("502") ||
        message.includes("503") ||
        message.includes("504")
    );
}

function delay(ms: number): Promise<void> {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

run().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : "Unknown critical error";
    log("Critical failure", { message });
    process.exitCode = 1;
});
