use std::num::NonZeroU64;
use std::time::Duration;

use crate::http::types::RequestHead;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ResolvedRequestPolicy {
    pub(crate) max_request_body_size: Option<NonZeroU64>,
    pub(crate) timeout_request_body_idle: Option<Duration>,
    pub(crate) h2_timeout_response_stall: Option<Duration>,
}

#[derive(Clone, Debug)]
pub(crate) struct RequestPolicies {
    default: ResolvedRequestPolicy,
    rules: Box<[RequestPolicyRule]>,
}

#[derive(Clone, Debug)]
struct RequestPolicyRule {
    selector: PathSelector,
    policy: ResolvedRequestPolicy,
}

#[derive(Clone, Debug)]
enum PathSelector {
    Exact(Box<str>),
    Subtree(Box<str>),
    Any,
}

impl RequestPolicies {
    pub(crate) fn new(
        default: ResolvedRequestPolicy,
        rules: Vec<(String, ResolvedRequestPolicy)>,
    ) -> Result<Self, String> {
        let rules = rules
            .into_iter()
            .map(|(path, policy)| {
                Ok(RequestPolicyRule {
                    selector: PathSelector::parse(&path)?,
                    policy,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(Self {
            default,
            rules: rules.into_boxed_slice(),
        })
    }

    pub(crate) const fn default_policy(&self) -> ResolvedRequestPolicy {
        self.default
    }

    pub(crate) fn resolve(&self, head: &RequestHead) -> ResolvedRequestPolicy {
        if self.rules.is_empty() || head.protocol_is_websocket() {
            return self.default;
        }
        let Some(path_and_query) = head.path_and_query() else {
            return self.default;
        };
        let path = path_and_query
            .as_str()
            .split_once('?')
            .map_or(path_and_query.as_str(), |(path, _)| path);
        self.rules
            .iter()
            .find(|rule| rule.selector.matches(path))
            .map_or(self.default, |rule| rule.policy)
    }
}

impl Default for RequestPolicies {
    fn default() -> Self {
        Self {
            default: ResolvedRequestPolicy {
                max_request_body_size: None,
                timeout_request_body_idle: None,
                h2_timeout_response_stall: None,
            },
            rules: Box::new([]),
        }
    }
}

impl PathSelector {
    fn parse(value: &str) -> Result<Self, String> {
        if !value.is_ascii() || value.bytes().any(|byte| matches!(byte, b'?' | b'#')) {
            return Err(format!("invalid request policy path {value:?}"));
        }
        if value == "/*" {
            return Ok(Self::Any);
        }
        if let Some(prefix) = value.strip_suffix("/*") {
            if prefix.contains('*') {
                return Err(format!(
                    "request policy path supports only one terminal `/*` wildcard: {value:?}"
                ));
            }
            let subtree = value
                .strip_suffix('*')
                .expect("terminal wildcard is present");
            validate_policy_path(subtree, value)?;
            return Ok(Self::Subtree(Box::from(subtree)));
        }
        if value.contains('*') {
            return Err(format!(
                "request policy path supports only a terminal `/*` wildcard: {value:?}"
            ));
        }
        validate_policy_path(value, value)?;
        Ok(Self::Exact(Box::from(value)))
    }

    fn matches(&self, path: &str) -> bool {
        match self {
            Self::Exact(exact) => path == exact.as_ref(),
            Self::Subtree(prefix) => path.starts_with(prefix.as_ref()),
            Self::Any => path.starts_with('/'),
        }
    }
}

fn validate_policy_path(path: &str, original: &str) -> Result<(), String> {
    if !path.starts_with('/') || path.parse::<http::uri::PathAndQuery>().is_err() {
        return Err(format!("invalid request policy path {original:?}"));
    }
    for escape in path.as_bytes().split(|byte| *byte == b'%').skip(1) {
        if !matches!(escape, [high, low, ..] if high.is_ascii_hexdigit() && low.is_ascii_hexdigit())
        {
            return Err(format!("invalid request policy path {original:?}"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::time::Duration;

    use http::Method;

    use super::{RequestPolicies, ResolvedRequestPolicy};
    use crate::http::header_meta::RequestHeaderMeta;
    use crate::http::types::{
        BytesStr, HttpVersion, Protocol, RequestAuthority, RequestHead, RequestHeaders,
        RequestTarget,
    };

    fn policy(
        size: Option<NonZeroU64>,
        body_idle: Option<u64>,
        stall: Option<u64>,
    ) -> ResolvedRequestPolicy {
        ResolvedRequestPolicy {
            max_request_body_size: size,
            timeout_request_body_idle: body_idle.map(Duration::from_secs),
            h2_timeout_response_stall: stall.map(Duration::from_secs),
        }
    }

    fn request(path_and_query: &'static str) -> RequestHead {
        RequestHead {
            http_version: HttpVersion::Http2,
            method: Method::POST,
            target: RequestTarget::normal(
                BytesStr::from_static("https"),
                BytesStr::from_static(path_and_query),
            ),
            headers: RequestHeaders::default(),
            header_meta: RequestHeaderMeta::default(),
        }
    }

    #[test]
    fn exact_and_subtree_paths_match_without_query_or_normalization() {
        let exact = policy(NonZeroU64::new(17), Some(3), Some(5));
        let subtree = policy(NonZeroU64::new(29), Some(7), Some(11));
        let fallback = policy(NonZeroU64::new(41), Some(13), Some(19));
        let policies = RequestPolicies::new(fallback, vec![
            ("/upload/%2F".to_owned(), exact),
            ("/example.Chat/*".to_owned(), subtree),
        ])
        .expect("valid request-policy paths compile");

        assert_eq!(policies.resolve(&request("/upload/%2F?x=1")), exact);
        assert_eq!(policies.resolve(&request("/example.Chat/Stream")), subtree);
        assert_eq!(
            policies.resolve(&request("/example.Chat/Stream?x=1")),
            subtree
        );
        assert_eq!(policies.resolve(&request("/example.chat/Stream")), fallback);
        assert_eq!(policies.resolve(&request("/example.Chat")), fallback);
        assert_eq!(policies.resolve(&request("/upload/%2f")), fallback);
    }

    #[test]
    fn first_matching_rule_wins_and_root_subtree_matches_normal_paths() {
        let first = policy(NonZeroU64::new(1), None, None);
        let second = policy(NonZeroU64::new(2), None, None);
        let fallback = policy(None, None, None);
        let policies = RequestPolicies::new(fallback, vec![
            ("/api/*".to_owned(), first),
            ("/*".to_owned(), second),
        ])
        .expect("valid request-policy paths compile");

        assert_eq!(policies.resolve(&request("/api/v1")), first);
        assert_eq!(policies.resolve(&request("/other")), second);
        assert_eq!(policies.resolve(&request("*")), fallback);
    }

    #[test]
    fn websocket_candidate_keeps_the_ordinary_policy() {
        let selected = policy(NonZeroU64::new(1), Some(2), Some(3));
        let fallback = policy(NonZeroU64::new(4), Some(5), Some(6));
        let policies = RequestPolicies::new(fallback, vec![("/*".to_owned(), selected)])
            .expect("valid request-policy paths compile");
        let websocket = RequestHead {
            http_version: HttpVersion::Http2,
            method: Method::CONNECT,
            target: RequestTarget::extended_connect(
                RequestAuthority::new(BytesStr::from_static("example.test:443")),
                Protocol::from("websocket"),
                BytesStr::from_static("https"),
                BytesStr::from_static("/chat"),
            ),
            headers: RequestHeaders::default(),
            header_meta: RequestHeaderMeta::default(),
        };

        assert_eq!(policies.resolve(&websocket), fallback);
    }

    #[test]
    fn invalid_paths_are_rejected_when_the_native_table_is_compiled() {
        let fallback = policy(None, None, None);
        for path in [
            "relative",
            "/has query",
            "/has?query",
            "/has#fragment",
            "/bad%",
            "/bad*wildcard",
            "/double/**",
            "/non-ascii/é",
        ] {
            assert!(
                RequestPolicies::new(fallback, vec![(path.to_owned(), fallback)]).is_err(),
                "accepted invalid request policy path {path:?}"
            );
        }
    }
}
