//! Delivery modes (spec §6.4). Only `Standard` messages are stored here;
//! Ephemeral and P2P traffic goes over gateway ops 20 and 22.

use std::fmt;
use std::str::FromStr;

use crate::error::DomainError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeliveryMode {
    Standard,
    Ephemeral,
    P2p,
}

impl DeliveryMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Ephemeral => "ephemeral",
            Self::P2p => "p2p",
        }
    }
}

impl fmt::Display for DeliveryMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for DeliveryMode {
    type Err = DomainError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "standard" => Ok(Self::Standard),
            "ephemeral" => Ok(Self::Ephemeral),
            "p2p" => Ok(Self::P2p),
            _ => Err(DomainError::invalid("delivery_mode", "unknown delivery mode")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_wire_names() {
        for mode in [DeliveryMode::Standard, DeliveryMode::Ephemeral, DeliveryMode::P2p] {
            assert_eq!(mode.as_str().parse::<DeliveryMode>().unwrap(), mode);
        }
        assert!("carrier-pigeon".parse::<DeliveryMode>().is_err());
    }
}
