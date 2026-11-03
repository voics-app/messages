//! Startup configuration, read from environment variables only (pinned
//! interface P2) and validated in one pass so every problem is reported at once.

use std::net::SocketAddr;
use std::time::Duration;

use discordy_messages_domain::WorkerId;
use discordy_messages_interface::attachment_urls::{DEFAULT_URL_TTL, MIN_KEY_BYTES};

pub const DEFAULT_BIND_ADDR: &str = "0.0.0.0:8082";
const DEFAULT_CORS_ALLOWED_ORIGINS: &str = "null,http://localhost:5173";

#[derive(Debug, thiserror::Error)]
#[error("invalid configuration:\n  - {}", .0.join("\n  - "))]
pub struct ConfigError(Vec<String>);

#[derive(Clone)]
pub struct Config {
    /// Product name (`BRAND_NAME`); only used to build the outbound unfurl user agent.
    pub brand_name: String,
    /// Public site URL (`BRAND_URL`), advertised in the unfurl user agent.
    pub brand_url: String,
    pub environment: Environment,
    pub log_level: String,
    pub bind_addr: SocketAddr,
    pub internal_service_token: String,
    pub jwt_public_key: String,
    pub worker: WorkerId,
    pub kafka_brokers: Vec<String>,
    pub scylla_nodes: Vec<String>,
    pub scylla_keyspace: String,
    pub api_internal_url: String,
    pub minio_endpoint: String,
    pub minio_access_key: String,
    pub minio_secret_key: String,
    pub attachments_bucket: String,
    pub cdn_public_url: String,
    /// HMAC key that signs attachment download URLs (at least 32 bytes).
    pub attachment_signing_key: String,
    pub attachment_url_ttl: Duration,
    /// Origins allowed by CORS; `null` is the origin of the packaged `file://` client.
    pub cors_allowed_origins: Vec<String>,
    /// PostgreSQL database of the message search index; search is disabled without it.
    pub search_database_url: Option<String>,
    /// Rebuild the index from stored history on startup when it is not complete yet.
    pub search_backfill_on_start: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Environment {
    Local,
    Production,
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let mut reader = Reader {
            lookup,
            problems: Vec::new(),
        };
        let environment = match reader.optional("DISCORDY_ENV", "local").as_str() {
            "local" => Environment::Local,
            "production" => Environment::Production,
            other => {
                reader.problem(format!("DISCORDY_ENV must be `local` or `production`, got `{other}`"));
                Environment::Local
            }
        };
        let bind_addr = reader.socket_addr("BIND_ADDR", DEFAULT_BIND_ADDR);
        let worker = reader.worker();
        let config = Self {
            brand_name: reader.optional("BRAND_NAME", "Discordy"), // brand-check: allow (brand source)
            brand_url: reader.optional("BRAND_URL", "https://discordy.voics.nl"), // brand-check: allow (brand source)
            environment,
            log_level: reader.optional("LOG_LEVEL", "info"),
            bind_addr,
            internal_service_token: reader.required("INTERNAL_SERVICE_TOKEN"),
            jwt_public_key: reader.required("JWT_PUBLIC_KEY").replace("\\n", "\n"),
            worker,
            kafka_brokers: reader.list("KAFKA_BROKERS"),
            scylla_nodes: reader.list("SCYLLA_NODES"),
            scylla_keyspace: reader.optional("SCYLLA_KEYSPACE", "discordy"),
            api_internal_url: reader.url("API_INTERNAL_URL"),
            minio_endpoint: reader.url("MINIO_ENDPOINT"),
            minio_access_key: reader.required("MINIO_ACCESS_KEY"),
            minio_secret_key: reader.required("MINIO_SECRET_KEY"),
            attachments_bucket: reader.optional("MINIO_BUCKET_ATTACHMENTS", "discordy-attachments"),
            cdn_public_url: reader.url("CDN_PUBLIC_URL"),
            attachment_signing_key: reader.signing_key("ATTACHMENT_SIGNING_KEY"),
            attachment_url_ttl: reader.seconds("ATTACHMENT_URL_TTL_SECONDS", DEFAULT_URL_TTL),
            cors_allowed_origins: reader.origins("CORS_ALLOWED_ORIGINS", DEFAULT_CORS_ALLOWED_ORIGINS),
            search_database_url: reader.value("SEARCH_DATABASE_URL"),
            search_backfill_on_start: reader.flag("SEARCH_BACKFILL_ON_START", true),
        };
        if reader.problems.is_empty() {
            Ok(config)
        } else {
            Err(ConfigError(reader.problems))
        }
    }
}

struct Reader<F> {
    lookup: F,
    problems: Vec<String>,
}

impl<F: Fn(&str) -> Option<String>> Reader<F> {
    fn problem(&mut self, message: String) {
        self.problems.push(message);
    }

    fn value(&self, name: &str) -> Option<String> {
        (self.lookup)(name)
            .map(|v| v.trim().to_owned())
            .filter(|v| !v.is_empty())
    }

    fn required(&mut self, name: &str) -> String {
        self.value(name).unwrap_or_else(|| {
            self.problem(format!("{name} is required"));
            String::new()
        })
    }

    fn optional(&self, name: &str, default: &str) -> String {
        self.value(name).unwrap_or_else(|| default.to_owned())
    }

    fn list(&mut self, name: &str) -> Vec<String> {
        let items: Vec<String> = self
            .required(name)
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .map(str::to_owned)
            .collect();
        if items.is_empty() && self.value(name).is_some() {
            self.problem(format!("{name} must list at least one host:port"));
        }
        items
    }

    fn flag(&mut self, name: &str, default: bool) -> bool {
        match self.value(name).as_deref() {
            None => default,
            Some("true" | "1") => true,
            Some("false" | "0") => false,
            Some(other) => {
                self.problem(format!("{name} must be true or false, got `{other}`"));
                default
            }
        }
    }

    fn origins(&self, name: &str, default: &str) -> Vec<String> {
        self.optional(name, default)
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .map(str::to_owned)
            .collect()
    }

    fn url(&mut self, name: &str) -> String {
        let value = self.required(name);
        if !value.is_empty() && !(value.starts_with("http://") || value.starts_with("https://")) {
            self.problem(format!("{name} must be an http(s) URL"));
        }
        value
    }

    fn signing_key(&mut self, name: &str) -> String {
        let value = self.required(name);
        if !value.is_empty() && value.len() < MIN_KEY_BYTES {
            self.problem(format!("{name} must be at least {MIN_KEY_BYTES} characters"));
        }
        value
    }

    fn seconds(&mut self, name: &str, default: Duration) -> Duration {
        match self.value(name) {
            None => default,
            Some(raw) => match raw.parse::<u64>() {
                Ok(seconds) if seconds > 0 => Duration::from_secs(seconds),
                _ => {
                    self.problem(format!("{name} must be a positive number of seconds, got `{raw}`"));
                    default
                }
            },
        }
    }

    fn socket_addr(&mut self, name: &str, default: &str) -> SocketAddr {
        let raw = self.optional(name, default);
        raw.parse().unwrap_or_else(|_| {
            self.problem(format!("{name} must be host:port, got `{raw}`"));
            SocketAddr::from(([0, 0, 0, 0], 0))
        })
    }

    fn worker(&mut self) -> WorkerId {
        let fallback = WorkerId::for_messages(*discordy_messages_domain::snowflake::MESSAGES_WORKER_RANGE.start())
            .expect("the range start is a valid worker");
        let raw = self.required("SNOWFLAKE_WORKER_ID");
        if raw.is_empty() {
            return fallback;
        }
        match raw.parse::<u16>().ok().map(WorkerId::for_messages) {
            Some(Ok(worker)) => worker,
            _ => {
                self.problem(format!(
                    "SNOWFLAKE_WORKER_ID must be an integer from 64 to 127, got `{raw}`"
                ));
                fallback
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn complete() -> HashMap<&'static str, &'static str> {
        HashMap::from([
            ("INTERNAL_SERVICE_TOKEN", "secret"),
            (
                "JWT_PUBLIC_KEY",
                "-----BEGIN PUBLIC KEY-----\\nabc\\n-----END PUBLIC KEY-----",
            ),
            ("SNOWFLAKE_WORKER_ID", "64"),
            ("KAFKA_BROKERS", "a:9092, b:9092"),
            ("SCYLLA_NODES", "scylla:9042"),
            ("API_INTERNAL_URL", "http://api:8080"),
            ("MINIO_ENDPOINT", "http://minio:9000"),
            ("MINIO_ACCESS_KEY", "key"),
            ("MINIO_SECRET_KEY", "secret"),
            ("CDN_PUBLIC_URL", "http://localhost:8000/cdn"),
            ("ATTACHMENT_SIGNING_KEY", "0123456789abcdef0123456789abcdef"),
        ])
    }

    fn load(vars: &HashMap<&str, &str>) -> Result<Config, ConfigError> {
        Config::from_lookup(|name| vars.get(name).map(|v| (*v).to_owned()))
    }

    #[test]
    fn defaults_apply_to_optional_variables() {
        let config = load(&complete()).unwrap();
        assert_eq!(config.scylla_keyspace, "discordy");
        assert_eq!(config.attachments_bucket, "discordy-attachments");
        assert_eq!(config.environment, Environment::Local);
        assert_eq!(config.bind_addr.port(), 8082);
        assert_eq!(config.kafka_brokers, vec!["a:9092", "b:9092"]);
        assert!(config.jwt_public_key.contains('\n'));
    }

    #[test]
    fn every_problem_is_reported_together() {
        let mut vars = complete();
        vars.remove("INTERNAL_SERVICE_TOKEN");
        vars.insert("SNOWFLAKE_WORKER_ID", "3");
        vars.insert("CDN_PUBLIC_URL", "localhost");
        let message = load(&vars).err().unwrap().to_string();
        assert!(message.contains("INTERNAL_SERVICE_TOKEN is required"));
        assert!(message.contains("SNOWFLAKE_WORKER_ID must be an integer from 64 to 127"));
        assert!(message.contains("CDN_PUBLIC_URL must be an http(s) URL"));
    }

    #[test]
    fn the_signing_key_is_required_and_long_enough() {
        let mut vars = complete();
        vars.insert("ATTACHMENT_SIGNING_KEY", "short");
        assert!(
            load(&vars)
                .err()
                .unwrap()
                .to_string()
                .contains("at least 32 characters")
        );
        vars.remove("ATTACHMENT_SIGNING_KEY");
        assert!(
            load(&vars)
                .err()
                .unwrap()
                .to_string()
                .contains("ATTACHMENT_SIGNING_KEY is required")
        );
    }

    #[test]
    fn the_url_ttl_defaults_to_a_day_and_can_be_overridden() {
        let mut vars = complete();
        assert_eq!(load(&vars).unwrap().attachment_url_ttl, Duration::from_secs(86_400));
        vars.insert("ATTACHMENT_URL_TTL_SECONDS", "600");
        assert_eq!(load(&vars).unwrap().attachment_url_ttl, Duration::from_secs(600));
        vars.insert("ATTACHMENT_URL_TTL_SECONDS", "0");
        assert!(
            load(&vars)
                .err()
                .unwrap()
                .to_string()
                .contains("ATTACHMENT_URL_TTL_SECONDS")
        );
    }

    #[test]
    fn cors_origins_default_to_the_packaged_client_and_dev_server() {
        let config = load(&complete()).unwrap();
        assert_eq!(config.cors_allowed_origins, vec!["null", "http://localhost:5173"]);
    }

    #[test]
    fn cors_origins_are_parsed_from_a_comma_list() {
        let mut vars = complete();
        vars.insert("CORS_ALLOWED_ORIGINS", " https://app.discordy.example , null ,");
        let config = load(&vars).unwrap();
        assert_eq!(
            config.cors_allowed_origins,
            vec!["https://app.discordy.example", "null"]
        );
    }

    #[test]
    fn search_is_optional_and_backfills_on_start_by_default() {
        let mut vars = complete();
        let config = load(&vars).unwrap();
        assert_eq!(config.search_database_url, None);
        assert!(config.search_backfill_on_start);
        vars.insert("SEARCH_DATABASE_URL", "postgres://u:p@db/discordy_search");
        vars.insert("SEARCH_BACKFILL_ON_START", "false");
        let config = load(&vars).unwrap();
        assert_eq!(
            config.search_database_url.as_deref(),
            Some("postgres://u:p@db/discordy_search")
        );
        assert!(!config.search_backfill_on_start);
        vars.insert("SEARCH_BACKFILL_ON_START", "maybe");
        assert!(
            load(&vars)
                .err()
                .unwrap()
                .to_string()
                .contains("SEARCH_BACKFILL_ON_START")
        );
    }

    #[test]
    fn unknown_environments_are_rejected() {
        let mut vars = complete();
        vars.insert("DISCORDY_ENV", "staging");
        assert!(load(&vars).err().unwrap().to_string().contains("DISCORDY_ENV"));
    }
}
