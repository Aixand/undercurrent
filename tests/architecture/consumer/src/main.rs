// SPDX-License-Identifier: Apache-2.0

use undercurrent_protocol::{
    CancelRuntimeAttemptResultDto, GetRuntimeAttemptStatusRequest, ProductOperation,
    RepairOperationAttemptRequest, RuntimeAttemptStatusResultDto,
    RuntimeCancellationStatusResultDto,
};
use undercurrent::LocalClientLimits;

fn main() {
    let contract = (
        ProductOperation::RepairOperationAttempt,
        RepairOperationAttemptRequest::new("attempt-consumer-boundary".to_owned())
            .expect("static attempt ID is valid"),
    );
    let client_types = (
        std::mem::size_of::<LocalClientLimits>(),
        std::mem::size_of::<RuntimeAttemptStatusResultDto>(),
        std::mem::size_of::<CancelRuntimeAttemptResultDto>(),
        std::mem::size_of::<RuntimeCancellationStatusResultDto>(),
    );
    let status_contract = (
        ProductOperation::GetRuntimeAttemptStatus,
        GetRuntimeAttemptStatusRequest::attached(
            "attempt-consumer-boundary".to_owned(),
            "1".repeat(64),
        )
        .expect("static attached attempt binding is valid"),
    );
    let cancellation_contract = (
        ProductOperation::CancelRuntimeAttempt,
        ProductOperation::GetRuntimeCancellationStatus,
    );
    std::hint::black_box((
        contract,
        status_contract,
        cancellation_contract,
        client_types,
    ));
}
