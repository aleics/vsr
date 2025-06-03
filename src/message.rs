use bincode::{
    Decode, Encode,
    config::Configuration,
    error::{DecodeError, EncodeError},
};

use crate::{OPERATION_SIZE_MAX, replica::Log};

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) enum Message {
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

impl Message {
    /// Encode the [`Message`] in binary, by appending the message with its size
    /// (framing), so that only the necessary bit size is used when decoding.
    pub(crate) fn encode(&self, config: Configuration) -> Result<Vec<u8>, EncodeError> {
        let encoded = bincode::encode_to_vec(self, config)?;

        // Append the encoded content with its length
        let mut frame = (encoded.len() as u32).to_be_bytes().to_vec();
        frame.extend_from_slice(&encoded);
        Ok(frame)
    }

    /// Decode the given bytes into a [`Message`] using the framing implemented
    /// in [`Message::encode`]
    pub(crate) fn decode(
        buf: &mut Vec<u8>,
        config: Configuration,
    ) -> Result<Option<Message>, DecodeError> {
        if buf.len() < 4 {
            return Ok(None);
        }

        // Read the first 4 bytes (`u32`) containing the size of the message
        let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        if buf.len() < 4 + len {
            return Ok(None);
        }

        // Decode the message
        let (message, _) = bincode::decode_from_slice(&buf[4..4 + len], config)?;
        buf.drain(..4 + len);
        Ok(Some(message))
    }
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct RequestMessage {
    pub(crate) view: usize,
    pub(crate) request_number: usize,
    pub(crate) client_id: usize,
    pub(crate) operation: Operation,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct PrepareMessage {
    pub(crate) view: usize,
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
    pub(crate) request: RequestMessage,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct PrepareOkMessage {
    pub(crate) view: usize,
    pub(crate) operation_number: usize,
    pub(crate) replica_number: usize,
    pub(crate) client_id: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct CommitMessage {
    pub(crate) replica_number: usize,
    pub(crate) view: usize,
    pub(crate) operation_number: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct GetStateMessage {
    pub(crate) view: usize,
    pub(crate) operation_number: usize,
    pub(crate) replica_number: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct NewStateMessage {
    pub(crate) view: usize,
    pub(crate) replica_number: usize,
    pub(crate) log_after_operation: Log,
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct StartViewChangeMessage {
    pub(crate) new_view: usize,
    pub(crate) replica_number: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct DoViewChangeMessage {
    pub(crate) old_view: usize,
    pub(crate) new_view: usize,
    pub(crate) log: Log,
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
    pub(crate) replica_number: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct StartViewMessage {
    pub(crate) replica_number: usize,
    pub(crate) view: usize,
    pub(crate) log: Log,
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct RecoveryMessage {
    pub(crate) replica_number: usize,
    pub(crate) nonce: u64,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct RecoveryResponseMessage {
    pub(crate) replica_number: usize,
    pub(crate) view: usize,
    pub(crate) nonce: u64,
    pub(crate) primary: Option<RecoveryPrimaryResponse>,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct RecoveryPrimaryResponse {
    pub(crate) log: Log,
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct ReplyMessage {
    pub(crate) view: usize,
    pub(crate) request_number: usize,
    pub(crate) result: Operation,
}

pub(crate) type Operation = [u8; OPERATION_SIZE_MAX];
