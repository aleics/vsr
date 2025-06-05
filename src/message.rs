use std::io::Cursor;

use bincode::{
    Decode, Encode,
    config::Configuration,
    error::{DecodeError, EncodeError},
};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{OPERATION_SIZE_MAX, replica::Log};

const HEADER_SIZE: usize = 4;

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
    pub(crate) fn encode(&self, config: Configuration) -> Result<Bytes, EncodeError> {
        let payload = bincode::encode_to_vec(self, config)?;
        let mut buf = BytesMut::with_capacity(payload.len() + HEADER_SIZE);

        // Append the encoded content with its length
        buf.put_u32(payload.len() as u32);
        buf.extend_from_slice(&payload);

        Ok(buf.freeze())
    }

    /// Decode the given bytes into a [`Message`] using the framing implemented
    /// in [`Message::encode`]
    pub(crate) fn decode(
        buf: &mut BytesMut,
        config: Configuration,
    ) -> Result<Option<Message>, DecodeError> {
        if buf.len() < HEADER_SIZE {
            return Ok(None);
        }

        // Read the first 4 bytes (`u32`) containing the size of the message
        let mut cursor = Cursor::new(&buf[..HEADER_SIZE]);
        let length = cursor.get_u32() as usize;

        if buf.len() < HEADER_SIZE + length {
            return Ok(None);
        }

        // Decode the message's payload
        buf.advance(HEADER_SIZE);
        let payload = buf.split_to(length);

        let (message, _) = bincode::decode_from_slice(&payload.freeze(), config)?;
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
