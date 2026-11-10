// SPDX-License-Identifier: Apache-2.0

//! Authorized active-catalog reads, linting, matching, and sample evaluation.
//!
//! List cursors retain the last profile name for a logical client, not a frozen
//! catalog generation. Every continuation reads the current catalog, so callers
//! needing a stable complete listing must restart it after a profile reload.

use undercurrent_application::DetectionProfileService;
use undercurrent_protocol::{
    GetProfileRequest, LintProfilesRequest, ListProfilesRequest, MatchProfileRequest,
    ProductOperation, ProductResponse, ProductSuccess, ProfilePageDto, TestProfileRequest,
};

use super::{ApplicationProductExecutor, error};
use crate::{ProductPresentation, ProductRequestContext, ProfilePageCursor, blocking::block_on};

impl ApplicationProductExecutor {
    pub(super) fn list_profiles(
        &self,
        context: &ProductRequestContext,
        request: ListProfilesRequest,
    ) -> ProductResponse {
        let operation = ProductOperation::ListProfiles;
        let page_size = match self.boundary.profile_page_size(&request) {
            Ok(value) => value.get(),
            Err(source) => return error::boundary(operation, None, source),
        };
        let service = match self.profile_service(operation) {
            Ok(service) => service,
            Err(response) => return response,
        };
        let profiles = match block_on(service.list()) {
            Ok(profiles) => profiles,
            Err(source) => return error::application(operation, None, source.category(), &source),
        };
        let after = match request.cursor.as_deref() {
            Some(token) => match self.cursors.profiles.resolve(&context.client_id, token) {
                Ok(cursor) => Some(cursor),
                Err(source) => return error::cursor(operation, source),
            },
            None => None,
        };
        let start = after.as_ref().map_or(0, |cursor| {
            profiles.partition_point(|profile| profile.name().as_str() <= cursor.after_name())
        });
        let end = start.saturating_add(page_size).min(profiles.len());
        let page = &profiles[start..end];
        let next_cursor = if end < profiles.len() {
            let Some(last) = page.last() else {
                return error::invalid_configuration(operation);
            };
            match self.cursors.profiles.register(
                context.client_id.clone(),
                ProfilePageCursor::new(last.name().as_str()),
            ) {
                Ok(cursor) => Some(cursor),
                Err(source) => return error::cursor(operation, source),
            }
        } else {
            None
        };
        success(ProductSuccess::ListProfiles(ProfilePageDto {
            items: page
                .iter()
                .map(ProductPresentation::profile_summary)
                .collect(),
            next_cursor,
        }))
    }

    pub(super) fn get_profile(&self, request: GetProfileRequest) -> ProductResponse {
        let operation = ProductOperation::GetProfile;
        let name = match self.boundary.profile_name(&request) {
            Ok(name) => name,
            Err(source) => return error::boundary(operation, None, source),
        };
        let service = match self.profile_service(operation) {
            Ok(service) => service,
            Err(response) => return response,
        };
        match block_on(service.inspect(&name)) {
            Ok(profile) => match ProductPresentation::profile_detail(&profile) {
                Ok(profile) => success(ProductSuccess::GetProfile(profile)),
                Err(source) => error::presentation(operation, source),
            },
            Err(source) => error::application(operation, None, source.category(), &source),
        }
    }

    pub(super) fn match_profile(&self, request: MatchProfileRequest) -> ProductResponse {
        let operation = ProductOperation::MatchProfile;
        let target = match self.boundary.profile_match_target(request) {
            Ok(target) => target,
            Err(source) => return error::boundary(operation, None, source),
        };
        let service = match self.profile_service(operation) {
            Ok(service) => service,
            Err(response) => return response,
        };
        match block_on(service.match_profile(&target)) {
            Ok(outcome) => success(ProductSuccess::MatchProfile(
                ProductPresentation::profile_match(&outcome),
            )),
            Err(source) => error::application(operation, None, source.category(), &source),
        }
    }

    pub(super) fn lint_profiles(&self, request: LintProfilesRequest) -> ProductResponse {
        let operation = ProductOperation::LintProfiles;
        let name = match self.boundary.profile_lint_name(&request) {
            Ok(name) => name,
            Err(source) => return error::boundary(operation, None, source),
        };
        let service = match self.profile_service(operation) {
            Ok(service) => service,
            Err(response) => return response,
        };
        let reports = match name {
            Some(name) => match block_on(service.lint(&name)) {
                Ok(report) => vec![report],
                Err(source) => {
                    return error::application(operation, None, source.category(), &source);
                }
            },
            None => match block_on(service.lint_all()) {
                Ok(reports) => reports,
                Err(source) => {
                    return error::application(operation, None, source.category(), &source);
                }
            },
        };
        success(ProductSuccess::LintProfiles(
            ProductPresentation::profile_lint(&reports),
        ))
    }

    pub(super) fn test_profile(&self, request: TestProfileRequest) -> ProductResponse {
        let operation = ProductOperation::TestProfile;
        let (name, sample) = match self.boundary.profile_test(request) {
            Ok(value) => value,
            Err(source) => return error::boundary(operation, None, source),
        };
        let service = match self.profile_service(operation) {
            Ok(service) => service,
            Err(response) => return response,
        };
        match block_on(service.test(&name, &[sample])) {
            Ok(report) => match ProductPresentation::profile_test(&report) {
                Ok(report) => success(ProductSuccess::TestProfile(report)),
                Err(source) => error::presentation(operation, source),
            },
            Err(source) => error::application(operation, None, source.category(), &source),
        }
    }

    fn profile_service(
        &self,
        operation: ProductOperation,
    ) -> Result<DetectionProfileService<'_>, ProductResponse> {
        DetectionProfileService::new(
            self.ports.reads.authorizer.as_ref(),
            self.ports.reads.profiles.as_ref(),
            self.ports.reads.patterns.as_ref(),
            self.ports.clock.as_ref(),
            self.ports.telemetry.as_ref(),
            self.ports.reads.profile_limits,
        )
        .map_err(|_| error::invalid_configuration(operation))
    }
}

fn success(value: ProductSuccess) -> ProductResponse {
    ProductResponse::Success(Box::new(value))
}
