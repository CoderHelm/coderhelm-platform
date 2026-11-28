//! What an AWS SDK error was, as a short kind for a log field.
//!
//! An SDK error printed with `%e` reads "service error" for every refusal
//! AWS answers, so an AccessDenied, a ValidationException and a throttle
//! all look the same in the logs. [`kind`] names the service's own error
//! code, or which part of the request failed when AWS never answered, and
//! [`detail`] is the whole chain of causes (the service's message, which
//! names the action or the attribute, never an item's values).

use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;

/// The service's error code (`AccessDeniedException`,
/// `ValidationException`, ...), or how the request failed without an
/// answer (`timeout`, `dispatch_io`, `dispatch_timeout`, `dispatch`,
/// `construction`, `response`).
pub fn kind<E, R>(e: &SdkError<E, R>) -> String
where
    E: ProvideErrorMetadata,
{
    match e {
        SdkError::ServiceError(s) => s.err().code().unwrap_or("service_error").to_string(),
        SdkError::TimeoutError(_) => "timeout".into(),
        SdkError::DispatchFailure(d) if d.is_timeout() => "dispatch_timeout".into(),
        SdkError::DispatchFailure(d) if d.is_io() => "dispatch_io".into(),
        SdkError::DispatchFailure(_) => "dispatch".into(),
        SdkError::ConstructionFailure(_) => "construction".into(),
        SdkError::ResponseError(_) => "response".into(),
        _ => "unknown".into(),
    }
}

/// The error and every cause under it, on one line.
pub fn detail<E, R>(e: &SdkError<E, R>) -> String
where
    E: std::error::Error + 'static,
    R: std::fmt::Debug,
{
    aws_smithy_types::error::display::DisplayErrorContext(e).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
    use aws_smithy_runtime_api::http::{Response, StatusCode};
    use aws_smithy_types::body::SdkBody;
    use aws_smithy_types::error::ErrorMetadata;

    fn service(code: &str, message: &str) -> SdkError<UpdateItemError, Response> {
        let meta = ErrorMetadata::builder().code(code).message(message).build();
        SdkError::service_error(
            UpdateItemError::generic(meta),
            Response::new(StatusCode::try_from(400).unwrap(), SdkBody::empty()),
        )
    }

    #[test]
    fn a_refusal_is_logged_by_its_code_not_as_service_error() {
        let e = service(
            "ValidationException",
            "Invalid UpdateExpression: Attribute name is a reserved keyword; reserved keyword: reads",
        );
        assert_eq!(e.to_string(), "service error", "what `%e` printed");
        assert_eq!(kind(&e), "ValidationException");
        assert!(
            detail(&e).contains("reserved keyword: reads"),
            "{}",
            detail(&e)
        );
        assert_eq!(
            kind(&service("AccessDeniedException", "no")),
            "AccessDeniedException"
        );
    }

    #[test]
    fn a_request_that_never_got_an_answer_says_how() {
        let e: SdkError<UpdateItemError, Response> = SdkError::timeout_error("slow");
        assert_eq!(kind(&e), "timeout");
        let e: SdkError<UpdateItemError, Response> = SdkError::construction_failure("bad");
        assert_eq!(kind(&e), "construction");
    }
}
