use bincode::{Decode, Encode};

use crate::{OPERATION_SIZE_MAX, replica::Log};

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub enum Message {
    Request(RequestMessage),
    Prepare(PrepareMessage),
    PrepareOk(PrepareOkMessage),
    Commit(CommitMessage),
    GetState(GetStateMessage),
    NewState(NewStateMessage),
    StartViewChange(StartViewChangeMessage),
    DoViewChange(DoViewChangeMessage),
    StartView(StartViewMessage),
    Recovery(RecoveryMessage),
    RecoveryResponse(RecoveryResponseMessage),
    Reply(ReplyMessage),
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct RequestMessage {
    pub view: usize,
    pub request_number: usize,
    pub client_id: usize,
    pub operation: Operation,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct PrepareMessage {
    pub(crate) view: usize,
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
    pub(crate) request: RequestMessage,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct PrepareOkMessage {
    pub(crate) view: usize,
    pub(crate) operation_number: usize,
    pub(crate) replica_number: usize,
    pub(crate) client_id: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct CommitMessage {
    pub(crate) replica_number: usize,
    pub(crate) view: usize,
    pub(crate) operation_number: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct GetStateMessage {
    pub(crate) view: usize,
    pub(crate) operation_number: usize,
    pub(crate) replica_number: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct NewStateMessage {
    pub(crate) view: usize,
    pub(crate) replica_number: usize,
    pub(crate) log_after_operation: Log,
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct StartViewChangeMessage {
    pub(crate) new_view: usize,
    pub(crate) replica_number: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct DoViewChangeMessage {
    pub(crate) old_view: usize,
    pub(crate) new_view: usize,
    pub(crate) log: Log,
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
    pub(crate) replica_number: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct StartViewMessage {
    pub(crate) replica_number: usize,
    pub(crate) view: usize,
    pub(crate) log: Log,
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct RecoveryMessage {
    pub(crate) replica_number: usize,
    pub(crate) nonce: u64,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct RecoveryResponseMessage {
    pub(crate) replica_number: usize,
    pub(crate) view: usize,
    pub(crate) nonce: u64,
    pub(crate) primary: Option<RecoveryPrimaryResponse>,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct RecoveryPrimaryResponse {
    pub(crate) log: Log,
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct ReplyMessage {
    pub(crate) view: usize,
    pub(crate) request_number: usize,
    pub(crate) result: Operation,
}

pub type Operation = [u8; OPERATION_SIZE_MAX];
