//! Uncached current notification policy. A malformed page fails closed.

use async_trait::async_trait;
use contracts::{NotificationRecipientDto, NotificationRecipientPageDto};
use discordy_messages_application::PortError;
use discordy_messages_application::ports::{NotificationRecipientPage, NotificationRecipients, RecipientQuery};
use discordy_messages_domain::{DeliveryMode, NotificationLevel, NotificationRecipient, Snowflake};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use super::{InternalApi, port_error};

pub struct HttpNotificationRecipients(pub InternalApi);

fn parse_id(raw: &str) -> Result<Snowflake, PortError> {
    let id = raw.parse::<Snowflake>().map_err(port_error)?;
    if id.value() > i64::MAX as u64 {
        return Err(port_error("notification recipient ID exceeds the database range"));
    }
    Ok(id)
}

fn recipient(dto: NotificationRecipientDto) -> Result<NotificationRecipient, PortError> {
    let level = match dto.level {
        0 => NotificationLevel::AllMessages,
        1 => NotificationLevel::OnlyMentions,
        2 => NotificationLevel::NoMessages,
        _ => return Err(port_error("invalid notification recipient level")),
    };
    Ok(NotificationRecipient {
        user_id: parse_id(&dto.user_id)?,
        role_ids: dto.role_ids.iter().map(|raw| parse_id(raw)).collect::<Result<_, _>>()?,
        online: dto.online,
        level,
        muted: dto.muted,
        suppress_everyone: dto.suppress_everyone,
        suppress_roles: dto.suppress_roles,
    })
}

fn page(dto: NotificationRecipientPageDto, query: RecipientQuery) -> Result<NotificationRecipientPage, PortError> {
    if parse_id(&dto.channel_id)? != query.channel_id || dto.recipients.len() > usize::from(query.limit) {
        return Err(port_error("invalid notification recipient page scope or size"));
    }
    OffsetDateTime::parse(&dto.evaluated_at, &Rfc3339).map_err(port_error)?;
    let next_after = dto.next_after.as_deref().map(parse_id).transpose()?;
    if next_after.is_some_and(|next| query.after.is_some_and(|after| next <= after)) {
        return Err(port_error("notification recipient cursor did not advance"));
    }
    let delivery_mode = dto.delivery_mode.parse::<DeliveryMode>().map_err(port_error)?;
    if delivery_mode != DeliveryMode::Standard && (!dto.recipients.is_empty() || next_after.is_some()) {
        return Err(port_error("private notification recipient page is not empty"));
    }
    let recipients: Vec<_> = dto.recipients.into_iter().map(recipient).collect::<Result<_, _>>()?;
    let mut previous = query.after;
    for recipient in &recipients {
        let id = recipient.user_id;
        if id == query.author_id
            || previous.is_some_and(|previous| id <= previous)
            || next_after.is_some_and(|next| id > next)
        {
            return Err(port_error("notification recipient page violates candidate order"));
        }
        previous = Some(id);
    }
    Ok(NotificationRecipientPage {
        guild_id: dto.guild_id.as_deref().map(parse_id).transpose()?,
        delivery_mode,
        recipients,
        next_after,
    })
}

#[async_trait]
impl NotificationRecipients for HttpNotificationRecipients {
    async fn page(&self, query: RecipientQuery) -> Result<NotificationRecipientPage, PortError> {
        if !(1..=200).contains(&query.limit) {
            return Err(port_error("notification recipient page limit must be in 1..200"));
        }
        let mut params = vec![
            ("author_id", query.author_id.to_string()),
            ("limit", query.limit.to_string()),
        ];
        if let Some(after) = query.after {
            params.push(("after", after.to_string()));
        }
        let response = self
            .0
            .get(&format!(
                "/internal/v1/channels/{}/notification-recipients",
                query.channel_id
            ))
            .query(&params)
            .send()
            .await
            .map_err(port_error)?;
        if !response.status().is_success() {
            return Err(port_error(format!(
                "notification recipients returned {}",
                response.status()
            )));
        }
        page(response.json().await.map_err(port_error)?, query)
    }
}
