//! `GET /guilds/{id}/messages/search` and `GET /channels/{id}/messages/search`.

use axum::Json;
use axum::extract::{RawQuery, State};
use contracts::MessageSearchResponse;
use discordy_messages_application::use_cases::{SearchOutcome, SearchScope};
use discordy_messages_domain::{HasFilter, HasSet, SearchFilters, SortBy, SortOrder};

use crate::authors::resolve_views;
use crate::dto::view_dto;
use crate::error::ApiError;
use crate::extract::{ApiPath, Caller, Id, parse_id};
use crate::state::AppState;

fn parse_bool(field: &str, raw: &str) -> Result<bool, ApiError> {
    match raw {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(ApiError::invalid(field, "must be true or false")),
    }
}

fn parse_number(field: &str, raw: &str) -> Result<u32, ApiError> {
    raw.parse()
        .map_err(|_| ApiError::invalid(field, "must be a non-negative integer"))
}

/// Parses the query string. Array parameters (`author_id`, `mentions`, `has`,
/// `channel_id`) are repeated keys, as in Discord's API; unknown keys are ignored.
pub fn parse_filters(query: Option<&str>) -> Result<SearchFilters, ApiError> {
    let mut filters = SearchFilters::default();
    let mut has = HasSet::default();
    for (key, value) in form_urlencoded::parse(query.unwrap_or_default().as_bytes()) {
        let value = value.as_ref();
        match key.as_ref() {
            "content" => filters.content = Some(value.to_owned()),
            "author_id" => filters.author_ids.push(parse_id("author_id", value)?),
            "mentions" => filters.mentions.push(parse_id("mentions", value)?),
            "channel_id" => filters.channel_ids.push(parse_id("channel_id", value)?),
            "has" => has.insert(value.parse::<HasFilter>()?),
            "min_id" => filters.min_id = Some(parse_id("min_id", value)?),
            "max_id" => filters.max_id = Some(parse_id("max_id", value)?),
            "pinned" => filters.pinned = Some(parse_bool("pinned", value)?),
            "sort_by" => filters.sort_by = value.parse::<SortBy>()?,
            "sort_order" => filters.sort_order = value.parse::<SortOrder>()?,
            "offset" => filters.offset = parse_number("offset", value)?,
            "limit" => filters.limit = parse_number("limit", value)?,
            "context" => filters.context = parse_number("context", value)?,
            _ => {}
        }
    }
    filters.has = has;
    filters.validated().map_err(ApiError::from)
}

async fn respond(
    state: &AppState,
    scope: SearchScope,
    caller: discordy_messages_domain::UserId,
    filters: SearchFilters,
) -> Result<Json<MessageSearchResponse>, ApiError> {
    let SearchOutcome {
        total_results,
        groups,
        index_building,
    } = state.use_cases.search.execute(scope, caller, filters).await?;
    let views: Vec<_> = groups.iter().flatten().map(|m| m.view.clone()).collect();
    let authors = resolve_views(state.users.as_ref(), &views).await;
    let messages = groups
        .iter()
        .map(|group| {
            group
                .iter()
                .map(|m| {
                    let mut dto = view_dto(&m.view, &state.urls, &authors);
                    dto.hit = m.hit.then_some(true);
                    dto
                })
                .collect()
        })
        .collect();
    Ok(Json(MessageSearchResponse {
        total_results,
        messages,
        index_building,
    }))
}

pub async fn guild(
    State(state): State<AppState>,
    Caller(caller): Caller,
    ApiPath(Id(guild_id)): ApiPath<Id>,
    RawQuery(query): RawQuery,
) -> Result<Json<MessageSearchResponse>, ApiError> {
    let filters = parse_filters(query.as_deref())?;
    respond(&state, SearchScope::Guild(guild_id), caller.user_id, filters).await
}

pub async fn channel(
    State(state): State<AppState>,
    Caller(caller): Caller,
    ApiPath(Id(channel_id)): ApiPath<Id>,
    RawQuery(query): RawQuery,
) -> Result<Json<MessageSearchResponse>, ApiError> {
    let filters = parse_filters(query.as_deref())?;
    respond(&state, SearchScope::Channel(channel_id), caller.user_id, filters).await
}

#[cfg(test)]
mod tests {
    use discordy_messages_domain::Snowflake;

    use super::*;

    #[test]
    fn parses_repeated_and_scalar_parameters() {
        let filters = parse_filters(Some(
            "content=hello%20world&author_id=1&author_id=2&mentions=3&has=image&has=link&channel_id=9\
             &min_id=10&max_id=20&pinned=true&sort_by=relevance&sort_order=asc&offset=50&limit=10&context=0&unknown=x",
        ))
        .unwrap();
        assert_eq!(filters.content.as_deref(), Some("hello world"));
        assert_eq!(filters.author_ids, vec![Snowflake::new(1), Snowflake::new(2)]);
        assert_eq!(filters.mentions, vec![Snowflake::new(3)]);
        assert!(filters.has.contains(HasFilter::Image) && filters.has.contains(HasFilter::Link));
        assert_eq!(filters.channel_ids, vec![Snowflake::new(9)]);
        assert_eq!(filters.min_id, Some(Snowflake::new(10)));
        assert_eq!(filters.max_id, Some(Snowflake::new(20)));
        assert_eq!(filters.pinned, Some(true));
        assert_eq!(filters.sort_by, SortBy::Relevance);
        assert_eq!(filters.sort_order, SortOrder::Asc);
        assert_eq!((filters.offset, filters.limit, filters.context), (50, 10, 0));
    }

    #[test]
    fn an_empty_query_uses_the_defaults() {
        let filters = parse_filters(None).unwrap();
        assert_eq!(filters, SearchFilters::default());
    }

    #[test]
    fn bad_values_are_rejected() {
        for query in [
            "author_id=abc",
            "has=gif",
            "pinned=maybe",
            "limit=26",
            "offset=-1",
            "sort_by=size",
        ] {
            assert!(parse_filters(Some(query)).is_err(), "{query}");
        }
    }
}
