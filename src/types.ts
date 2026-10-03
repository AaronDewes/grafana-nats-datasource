import {DataQuery, DataSourceJsonData, SelectableValue} from '@grafana/data';
// These need to be synced with types.go

export type QueryTypes = "REQUEST_REPLY" | "SUBSCRIBE" | "SCRIPT" | "KV";
export interface MyQuery extends DataQuery {
    queryType: QueryTypes;
    natsSubject: string;
    requestTimeout: string;
    requestData: string;

    // for KV only
    kvBucket?: string;
    kvKey?: string;
    kvHistory?: boolean;

    // for REQUEST_REPLY and SUBSCRIBE, gets each individual message and can transform it.
    // for KV, gets each individual entry and can transform it.
    // for SCRIPT, can take control of any flow.
    jsFn: string;
}

export const QueryTypeOptions: Array<SelectableValue<QueryTypes>> = [
    {
        label: "Request/Reply",
        value: "REQUEST_REPLY",
        description: "Send a NATS request and wait for its reply."
    },
    {
        label: "Subscribe",
        value: "SUBSCRIBE",
        description: "Subscribe to a topic (wildcards allowed), and render them in a streaming fashion"
    },
    {
        label: "Key/Value",
        value: "KV",
        description: "Read the entries of a JetStream Key/Value bucket, optionally including their history."
    },
    {
        label: "Free-Form Script (advanced)",
        value: "SCRIPT",
        description: "Orchestrate complex interactions with NATS, like doing requests based on other responses; or reducing multiple responses to a single dataset."
    }
];

export const DEFAULT_QUERY: Partial<MyQuery> = {
    queryType: "REQUEST_REPLY",
    requestTimeout: "5s"
};

/**
 * The query types that can be used for annotations. An annotation is a one-shot query over the
 * dashboard's time range, so "SUBSCRIBE" does not apply: a subscription never completes.
 */
export const AnnotationQueryTypes: QueryTypes[] = ["REQUEST_REPLY", "SCRIPT", "KV"];

export const DEFAULT_ANNOTATION_QUERY: Partial<MyQuery> = {
    queryType: "REQUEST_REPLY",
    requestTimeout: "5s",
    jsFn: `// An annotation needs a "time" column. "timeEnd", "title", "text" and "tags" are optional.
// Use a JS Date for time and timeEnd, and a comma-separated string for tags.
return JSON.parse(msg.Data).map((event) => ({
    time: new Date(event.timestamp),
    title: event.title,
    text: event.message,
    tags: "nats"
}));`
};


type AuthenticationModes = "NONE" | "NKEY" | "USERPASS" | "JWT";

export const AuthenticationOptions = [
    {
        label: "no authentication",
        value: "NONE"
    }, {
        label: "NKEY based authentication",
        value: "NKEY"
    }, {
        label: "User / password authentication",
        value: "USERPASS"
    }, {
        label: "JWT based authentication",
        value: "JWT"
    }
];

/**
 * These are options configured for each DataSource instance
 */
export interface MyDataSourceOptions extends DataSourceJsonData {
    natsUrl?: string;
    authentication: AuthenticationModes;
    nkey?: string;
    username?: string;
}

// These need to be synced with types.go

/**
 * Value that is used in the backend, but never sent over HTTP to the frontend.
 * In the UI, they are write-only.
 */
export interface MySecureJsonData {
    nkeySeed?: string;
    password?: string;
    jwt?: string;
}
