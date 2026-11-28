//! Where a request's time went: every AWS call made while a request is
//! being answered, by service and operation, with how long it took, and the
//! ones still waiting for an answer.
//!
//! [`CallClock`] is an SDK interceptor; a client built with it records each
//! call into the [`Calls`] of the task it runs in (see [`within`]). Outside
//! [`within`] it records nothing. Only names and durations are kept, never
//! a request's or an answer's content.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::context::{
    BeforeSerializationInterceptorContextRef, FinalizerInterceptorContextRef,
};
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::orchestrator::Metadata;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::config_bag::{ConfigBag, Storable, StoreReplace};

tokio::task_local! {
    static CALLS: Calls;
}

#[derive(Default)]
struct Book {
    /// Calls that finished: `(service.operation, ms)`.
    done: Vec<(String, u64)>,
    /// Calls still waiting: `(id, service.operation, started)`.
    open: Vec<(u64, String, Instant)>,
    next: u64,
}

/// The AWS calls of one request.
#[derive(Clone, Default)]
pub struct Calls(Arc<Mutex<Book>>);

/// One call's place in [`Calls`], kept in the call's own config bag.
#[derive(Clone, Debug)]
struct Started(u64);

impl Storable for Started {
    type Storer = StoreReplace<Self>;
}

/// Where the time went, as log fields.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Spent {
    /// Calls that finished.
    pub calls: usize,
    /// Their time added up.
    pub calls_ms: u64,
    /// The slowest one that finished: `Service.Operation=ms`.
    pub slowest: String,
    /// Calls still waiting for an answer, longest first: `Service.Operation=ms`.
    pub waiting: Vec<String>,
}

impl Calls {
    fn start(&self, name: String) -> u64 {
        let mut b = self.0.lock().unwrap_or_else(|p| p.into_inner());
        b.next += 1;
        let id = b.next;
        b.open.push((id, name, Instant::now()));
        id
    }

    fn finish(&self, id: u64) {
        let mut b = self.0.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(i) = b.open.iter().position(|(x, _, _)| *x == id) {
            let (_, name, at) = b.open.remove(i);
            b.done.push((name, at.elapsed().as_millis() as u64));
        }
    }

    /// The calls so far.
    pub fn spent(&self) -> Spent {
        let b = self.0.lock().unwrap_or_else(|p| p.into_inner());
        let slowest = b
            .done
            .iter()
            .max_by_key(|(_, ms)| *ms)
            .map(|(n, ms)| format!("{n}={ms}"))
            .unwrap_or_default();
        let mut open: Vec<(String, u64)> = b
            .open
            .iter()
            .map(|(_, n, at)| (n.clone(), at.elapsed().as_millis() as u64))
            .collect();
        open.sort_by_key(|b| std::cmp::Reverse(b.1));
        Spent {
            calls: b.done.len(),
            calls_ms: b.done.iter().map(|(_, ms)| ms).sum(),
            slowest,
            waiting: open
                .into_iter()
                .map(|(n, ms)| format!("{n}={ms}"))
                .collect(),
        }
    }
}

/// Runs `fut` with `calls` recording the AWS calls it makes.
pub async fn within<F: std::future::Future>(calls: Calls, fut: F) -> F::Output {
    CALLS.scope(calls, fut).await
}

/// The SDK interceptor: add it to a client's config (`.interceptor(CallClock)`).
#[derive(Debug, Clone, Copy, Default)]
pub struct CallClock;

impl Intercept for CallClock {
    fn name(&self) -> &'static str {
        "CallClock"
    }

    // Before serialization, not before execution: the operation's name
    // (its Metadata) is in the config bag only once its own runtime
    // plugin has run. Nothing is sent before this hook either way.
    fn read_before_serialization(
        &self,
        _ctx: &BeforeSerializationInterceptorContextRef<'_>,
        _rc: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let name = cfg
            .load::<Metadata>()
            .map(|m| format!("{}.{}", m.service(), m.name()))
            .unwrap_or_else(|| "aws".to_string());
        if let Ok(id) = CALLS.try_with(|c| c.start(name)) {
            cfg.interceptor_state().store_put(Started(id));
        }
        Ok(())
    }

    fn read_after_execution(
        &self,
        _ctx: &FinalizerInterceptorContextRef<'_>,
        _rc: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        if let Some(Started(id)) = cfg.load::<Started>().cloned() {
            let _ = CALLS.try_with(|c| c.finish(id));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
    use aws_smithy_runtime_api::client::http::{
        HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpClient,
        SharedHttpConnector,
    };
    use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
    use std::time::Duration;

    /// An HTTP client whose answers never come, or come at once (`ok`).
    #[derive(Debug, Clone)]
    struct Net {
        ok: bool,
    }

    impl HttpConnector for Net {
        fn call(&self, _r: HttpRequest) -> HttpConnectorFuture {
            if self.ok {
                HttpConnectorFuture::ready(Ok(aws_smithy_runtime_api::http::Response::new(
                    200.try_into().unwrap(),
                    aws_smithy_types::body::SdkBody::from("{}"),
                )))
            } else {
                HttpConnectorFuture::new(std::future::pending())
            }
        }
    }

    impl HttpClient for Net {
        fn http_connector(
            &self,
            _s: &HttpConnectorSettings,
            _c: &RuntimeComponents,
        ) -> SharedHttpConnector {
            SharedHttpConnector::new(self.clone())
        }
    }

    fn dynamo(ok: bool) -> aws_sdk_dynamodb::Client {
        aws_sdk_dynamodb::Client::from_conf(
            aws_sdk_dynamodb::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .region(Region::new("us-east-1"))
                .credentials_provider(Credentials::new("a", "b", None, None, "test"))
                .http_client(SharedHttpClient::new(Net { ok }))
                .interceptor(CallClock)
                .build(),
        )
    }

    #[tokio::test]
    async fn a_call_that_never_answers_is_named_as_waiting() {
        let calls = Calls::default();
        let c = dynamo(false);
        let r = tokio::time::timeout(
            Duration::from_millis(300),
            within(calls.clone(), async {
                c.get_item().table_name("t").send().await
            }),
        )
        .await;
        assert!(r.is_err(), "it never answered");
        let s = calls.spent();
        assert_eq!(s.calls, 0);
        assert_eq!(s.waiting.len(), 1);
        assert!(s.waiting[0].starts_with("DynamoDB.GetItem="), "{s:?}");
    }

    #[tokio::test]
    async fn finished_calls_are_added_up() {
        let calls = Calls::default();
        let c = dynamo(true);
        within(calls.clone(), async {
            let _ = c.get_item().table_name("t").send().await;
            let _ = c.put_item().table_name("t").send().await;
        })
        .await;
        let s = calls.spent();
        assert_eq!((s.calls, s.waiting.len()), (2, 0), "{s:?}");
        assert!(s.slowest.starts_with("DynamoDB."), "{s:?}");
    }

    #[tokio::test]
    async fn outside_a_request_nothing_is_recorded() {
        let _ = dynamo(true).get_item().table_name("t").send().await;
    }
}
