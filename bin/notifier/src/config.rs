//! Startup configuration from environment variables, validated in one pass so every problem is
//! reported at once. The APNs key comes from a mounted file (`APNS_KEY_FILE`) or, for local runs,
//! the variable `APNS_KEY`; the key id and team id are plain settings.

use std::net::SocketAddr;
use std::time::Duration;

pub const DEFAULT_BIND_ADDR: &str = "0.0.0.0:8085";
/// The iOS bundle identifier, which is also the APNs topic.
pub const DEFAULT_APNS_TOPIC: &str = "nl.voics.app";

#[derive(Debug, thiserror::Error)]
#[error("invalid configuration:\n  - {}", .0.join("\n  - "))]
pub struct ConfigError(Vec<String>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApnsEnvironment {
    Production,
    Sandbox,
}

#[derive(Debug, Clone)]
pub struct ApnsCredentials {
    /// The PKCS#8 PEM text of the `.p8` key.
    pub key_pem: String,
    pub key_id: String,
    pub team_id: String,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub log_level: String,
    pub bind_addr: SocketAddr,
    pub kafka_brokers: Vec<String>,
    pub api_internal_url: String,
    pub internal_service_token: String,
    /// APNs credentials; `None` when none of them is set (iOS tokens are then skipped).
    pub apns: Option<ApnsCredentials>,
    pub apns_topic: String,
    /// The whole Firebase service-account JSON; `None` skips Android tokens.
    pub fcm_service_account: Option<String>,
    /// Forces every token to one APNs host; unset sends each token to the host it was issued for.
    pub apns_environment: Option<ApnsEnvironment>,
    pub queue_capacity: usize,
    pub max_in_flight: usize,
    pub max_attempts: u32,
    pub base_backoff: Duration,
    pub max_backoff: Duration,
    pub max_event_age: Duration,
    pub dedupe_capacity: usize,
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
            .map(|item| item.trim().to_owned())
            .filter(|item| !item.is_empty())
            .collect();
        if items.is_empty() && self.value(name).is_some() {
            self.problem(format!("{name} must list at least one address"));
        }
        items
    }

    fn url(&mut self, name: &str) -> String {
        let value = self.required(name);
        if !value.is_empty() && !(value.starts_with("http://") || value.starts_with("https://")) {
            self.problem(format!("{name} must be an http(s) URL"));
        }
        value
    }

    fn number(&mut self, name: &str, default: u64, min: u64) -> u64 {
        let Some(raw) = self.value(name) else {
            return default;
        };
        match raw.parse::<u64>() {
            Ok(n) if n >= min => n,
            _ => {
                self.problem(format!("{name} must be a whole number of at least {min}, got `{raw}`"));
                default
            }
        }
    }

    fn socket_addr(&mut self, name: &str, default: &str) -> SocketAddr {
        let raw = self.optional(name, default);
        raw.parse().unwrap_or_else(|_| {
            self.problem(format!("{name} must be host:port, got `{raw}`"));
            SocketAddr::from(([0, 0, 0, 0], 8085))
        })
    }

    fn apns_key(&mut self, read_file: &dyn Fn(&str) -> std::io::Result<String>) -> String {
        if let Some(path) = self.value("APNS_KEY_FILE") {
            return match read_file(&path) {
                Ok(pem) if pem.contains("PRIVATE KEY") => pem,
                Ok(_) => {
                    self.problem(format!("APNS_KEY_FILE ({path}) does not contain a PEM private key"));
                    String::new()
                }
                Err(error) => {
                    self.problem(format!("APNS_KEY_FILE ({path}) cannot be read: {error}"));
                    String::new()
                }
            };
        }
        match self.value("APNS_KEY") {
            Some(pem) if pem.contains("PRIVATE KEY") => pem,
            Some(_) => {
                self.problem("APNS_KEY does not contain a PEM private key".to_owned());
                String::new()
            }
            None => {
                self.problem("APNS_KEY_FILE (or APNS_KEY) is required".to_owned());
                String::new()
            }
        }
    }

    /// APNs is optional as a whole: no variable set means no APNs. A partial setup is reported.
    fn apns(&mut self, read_file: &dyn Fn(&str) -> std::io::Result<String>) -> Option<ApnsCredentials> {
        let any = ["APNS_KEY_FILE", "APNS_KEY", "APNS_KEY_ID", "APNS_TEAM_ID"]
            .iter()
            .any(|name| self.value(name).is_some());
        if !any {
            return None;
        }
        let key_pem = self.apns_key(read_file);
        Some(ApnsCredentials {
            key_pem,
            key_id: self.required("APNS_KEY_ID"),
            team_id: self.required("APNS_TEAM_ID"),
        })
    }

    /// FCM is optional: `NOTIFIER_FCM_SERVICE_ACCOUNT` holds the whole JSON, or
    /// `FCM_SERVICE_ACCOUNT_FILE` names a mounted file.
    fn fcm(&mut self, read_file: &dyn Fn(&str) -> std::io::Result<String>) -> Option<String> {
        let json = match self.value("FCM_SERVICE_ACCOUNT_FILE") {
            Some(path) => match read_file(&path) {
                Ok(text) => text,
                Err(error) => {
                    self.problem(format!("FCM_SERVICE_ACCOUNT_FILE ({path}) cannot be read: {error}"));
                    return None;
                }
            },
            None => self.value("NOTIFIER_FCM_SERVICE_ACCOUNT")?,
        };
        match serde_json::from_str::<serde_json::Value>(&json) {
            Ok(value) if value["project_id"].is_string() && value["private_key"].is_string() => Some(json),
            _ => {
                self.problem(
                    "the FCM service account must be the JSON key file with project_id and private_key".to_owned(),
                );
                None
            }
        }
    }
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::load(|name| std::env::var(name).ok(), &|path| std::fs::read_to_string(path))
    }

    pub fn load(
        lookup: impl Fn(&str) -> Option<String>,
        read_file: &dyn Fn(&str) -> std::io::Result<String>,
    ) -> Result<Self, ConfigError> {
        let mut reader = Reader {
            lookup,
            problems: Vec::new(),
        };
        let apns_environment = match reader.value("APNS_ENVIRONMENT").as_deref() {
            None => None,
            Some("production") => Some(ApnsEnvironment::Production),
            Some("sandbox") => Some(ApnsEnvironment::Sandbox),
            Some(other) => {
                reader.problem(format!(
                    "APNS_ENVIRONMENT must be `production` or `sandbox`, got `{other}`"
                ));
                None
            }
        };
        let queue_capacity = reader.number("PUSH_QUEUE_CAPACITY", 10_000, 1) as usize;
        let max_in_flight = reader.number("PUSH_MAX_IN_FLIGHT", 100, 1) as usize;
        let config = Self {
            log_level: reader.optional("LOG_LEVEL", "info"),
            bind_addr: reader.socket_addr("BIND_ADDR", DEFAULT_BIND_ADDR),
            kafka_brokers: reader.list("KAFKA_BROKERS"),
            api_internal_url: reader.url("API_INTERNAL_URL"),
            internal_service_token: reader.required("INTERNAL_SERVICE_TOKEN"),
            apns: reader.apns(read_file),
            apns_topic: reader.optional("APNS_TOPIC", DEFAULT_APNS_TOPIC),
            fcm_service_account: reader.fcm(read_file),
            apns_environment,
            queue_capacity,
            max_in_flight,
            max_attempts: reader.number("PUSH_MAX_ATTEMPTS", 3, 1) as u32,
            base_backoff: Duration::from_millis(reader.number("PUSH_BASE_BACKOFF_MS", 500, 1)),
            max_backoff: Duration::from_millis(reader.number("PUSH_MAX_BACKOFF_MS", 10_000, 1)),
            max_event_age: Duration::from_secs(reader.number("PUSH_MAX_EVENT_AGE_SECS", 120, 1)),
            dedupe_capacity: reader.number("PUSH_DEDUPE_CAPACITY", 10_000, 1) as usize,
        };
        if config.base_backoff > config.max_backoff {
            reader.problem("PUSH_BASE_BACKOFF_MS must not exceed PUSH_MAX_BACKOFF_MS".to_owned());
        }
        if reader.problems.is_empty() {
            Ok(config)
        } else {
            Err(ConfigError(reader.problems))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    const PEM: &str = "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n";

    fn base() -> HashMap<&'static str, &'static str> {
        HashMap::from([
            ("KAFKA_BROKERS", "redpanda:9092"),
            ("API_INTERNAL_URL", "http://api:8080"),
            ("INTERNAL_SERVICE_TOKEN", "t"),
            ("APNS_KEY_FILE", "/secrets/AuthKey.p8"),
            ("APNS_KEY_ID", "KEYID12345"),
            ("APNS_TEAM_ID", "TEAMID1234"),
        ])
    }

    fn load(vars: &HashMap<&'static str, &'static str>, file: &str) -> Result<Config, ConfigError> {
        let file = file.to_owned();
        Config::load(|name| vars.get(name).map(|v| (*v).to_owned()), &move |_| {
            Ok(file.clone())
        })
    }

    #[test]
    fn a_complete_environment_loads_with_defaults() {
        let config = load(&base(), PEM).unwrap();
        assert_eq!(config.apns_topic, "nl.voics.app");
        assert_eq!(config.apns_environment, None);
        assert_eq!(config.queue_capacity, 10_000);
        assert_eq!(config.max_in_flight, 100);
        assert_eq!(config.max_attempts, 3);
        assert_eq!(config.kafka_brokers, vec!["redpanda:9092"]);
        assert!(config.apns.unwrap().key_pem.contains("PRIVATE KEY"));
        assert_eq!(config.fcm_service_account, None);
    }

    #[test]
    fn every_problem_is_reported_at_once() {
        let error = load(&HashMap::new(), PEM).unwrap_err().to_string();
        for name in ["KAFKA_BROKERS", "API_INTERNAL_URL", "INTERNAL_SERVICE_TOKEN"] {
            assert!(error.contains(name), "{error}");
        }
        assert!(!error.contains("APNS"), "APNs is optional: {error}");
    }

    #[test]
    fn a_partial_apns_setup_is_reported() {
        let mut vars = base();
        vars.remove("APNS_KEY_ID");
        vars.remove("APNS_TEAM_ID");
        let error = load(&vars, PEM).unwrap_err().to_string();
        assert!(error.contains("APNS_KEY_ID"), "{error}");
        assert!(error.contains("APNS_TEAM_ID"), "{error}");
    }

    #[test]
    fn apns_may_be_absent_and_fcm_alone_is_enough() {
        let mut vars = base();
        for name in ["APNS_KEY_FILE", "APNS_KEY_ID", "APNS_TEAM_ID"] {
            vars.remove(name);
        }
        assert!(load(&vars, PEM).unwrap().apns.is_none());
        let account = r#"{"project_id":"p","private_key":"k","client_email":"e"}"#;
        vars.insert("NOTIFIER_FCM_SERVICE_ACCOUNT", account);
        assert_eq!(load(&vars, PEM).unwrap().fcm_service_account.as_deref(), Some(account));
    }

    #[test]
    fn a_malformed_fcm_service_account_is_reported_without_echoing_it() {
        let mut vars = base();
        vars.insert("NOTIFIER_FCM_SERVICE_ACCOUNT", "super-secret-not-json");
        let error = load(&vars, PEM).unwrap_err().to_string();
        assert!(error.contains("FCM service account"), "{error}");
        assert!(!error.contains("super-secret"), "{error}");
    }

    #[test]
    fn numbers_and_environment_are_validated() {
        let mut vars = base();
        vars.insert("PUSH_QUEUE_CAPACITY", "0");
        vars.insert("PUSH_MAX_IN_FLIGHT", "many");
        vars.insert("APNS_ENVIRONMENT", "staging");
        vars.insert("PUSH_BASE_BACKOFF_MS", "9000");
        vars.insert("PUSH_MAX_BACKOFF_MS", "1000");
        let error = load(&vars, PEM).unwrap_err().to_string();
        assert!(error.contains("PUSH_QUEUE_CAPACITY"), "{error}");
        assert!(error.contains("PUSH_MAX_IN_FLIGHT"), "{error}");
        assert!(error.contains("APNS_ENVIRONMENT"), "{error}");
        assert!(error.contains("PUSH_BASE_BACKOFF_MS must not exceed"), "{error}");
    }

    #[test]
    fn the_key_must_look_like_a_pem_and_be_readable() {
        let error = load(&base(), "just text").unwrap_err().to_string();
        assert!(error.contains("does not contain a PEM private key"), "{error}");
        let vars = base();
        let unreadable = Config::load(|name| vars.get(name).map(|v| (*v).to_owned()), &|_| {
            Err(std::io::Error::new(std::io::ErrorKind::NotFound, "missing"))
        })
        .unwrap_err()
        .to_string();
        assert!(unreadable.contains("cannot be read"), "{unreadable}");
    }

    #[test]
    fn an_inline_key_and_a_forced_environment_are_accepted() {
        let mut vars = base();
        vars.remove("APNS_KEY_FILE");
        vars.insert("APNS_KEY", PEM);
        vars.insert("APNS_ENVIRONMENT", "sandbox");
        let config = load(&vars, "unused").unwrap();
        assert_eq!(config.apns_environment, Some(ApnsEnvironment::Sandbox));
    }
}
