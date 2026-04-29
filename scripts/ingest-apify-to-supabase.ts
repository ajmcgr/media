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

    if (datasetIds.length === 0) {
        throw new Error("APIFY_DATASET_IDS did not contain any dataset ids");
    }

    return {
        apifyToken: readRequiredEnv("APIFY_TOKEN"),
        datasetIds,
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

function coalesceString(record: Record<string, unknown>, keys: string[]): string | undefined {
    for (const key of keys) {
        const value = asString(record[key]);
        if (value) {
            return value;
        }
    }
    return undefined;
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
            "youtube_url",
            "youtube",
            "channel_url",
            "rec_id",
            "creator_category",
            "niche",
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
    ]);
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
    const name = coalesceString(record, ["name", "full_name", "creator_name", "channel_name"]);
    const email = normalizeEmail(coalesceString(record, ["email", "contact_email"]));
    const igHandle = normalizeHandle(
        coalesceString(record, ["ig_handle", "instagram_handle", "instagram"]),
    );
    const youtubeUrl = normalizeWebsite(
        coalesceString(record, ["youtube_url", "youtube", "channel_url"]),
    );
    const recId = coalesceString(record, ["rec_id", "record_id", "creator_id"]);
    const website = normalizeWebsite(coalesceString(record, ["website", "domain", "site"]));
    const category = coalesceString(record, ["category", "creator_category", "niche"]);

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
    const normalized = isCreatorLike(record)
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

async function fetchAllRecords(config: RuntimeConfig): Promise<NormalizedRecord[]> {
    const normalizedRecords: NormalizedRecord[] = [];

    for (const datasetId of config.datasetIds) {
        const remainingBudget = config.maxRecordsPerRun - stats.rawPulled;
        if (remainingBudget <= 0) {
            break;
        }

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
        grouped[getConflictStrategy(record)].push(record);
    }
    return grouped;
}

async function run(): Promise<void> {
    const startedAt = Date.now();
    const config = getRuntimeConfig();

    log("Starting ingestion worker", {
        datasetIds: config.datasetIds,
        maxRecordsPerRun: config.maxRecordsPerRun,
        batchSize: config.batchSize,
        dryRun: config.dryRun,
    });

    const records = await fetchAllRecords(config);
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
    operation: () => Promise<T>,
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
