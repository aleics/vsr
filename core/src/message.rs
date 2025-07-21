use std::io::Cursor;

use bincode::{
    BorrowDecode, Decode, Encode,
    config::Configuration,
    de::{BorrowDecoder, Decoder},
    error::{DecodeError, EncodeError},
};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{ReplicaId, replica::Log};

pub(crate) const MESSAGE_SIZE_MAX: usize = 8 * 1024;
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
        if payload.is_empty() {
            return Ok(Bytes::new());
        }

        let message_size = payload.len() + HEADER_SIZE;
        let mut buf = BytesMut::with_capacity(message_size);

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
    pub(crate) view: ReplicaId,
    pub(crate) request_number: usize,
    pub(crate) client_id: usize,
    pub(crate) operation: Operation,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct PrepareMessage {
    pub(crate) view: ReplicaId,
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
    pub(crate) request: RequestMessage,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct PrepareOkMessage {
    pub(crate) view: ReplicaId,
    pub(crate) operation_number: usize,
    pub(crate) replica_number: ReplicaId,
    pub(crate) client_id: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct CommitMessage {
    pub(crate) replica_number: ReplicaId,
    pub(crate) view: ReplicaId,
    pub(crate) operation_number: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct GetStateMessage {
    pub(crate) view: ReplicaId,
    pub(crate) operation_number: usize,
    pub(crate) replica_number: ReplicaId,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct NewStateMessage {
    pub(crate) view: ReplicaId,
    pub(crate) replica_number: ReplicaId,
    pub(crate) log_after_operation: Log,
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct StartViewChangeMessage {
    pub(crate) new_view: ReplicaId,
    pub(crate) replica_number: ReplicaId,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct DoViewChangeMessage {
    pub(crate) old_view: ReplicaId,
    pub(crate) new_view: ReplicaId,
    pub(crate) log: Log,
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
    pub(crate) replica_number: ReplicaId,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct StartViewMessage {
    pub(crate) replica_number: ReplicaId,
    pub(crate) view: ReplicaId,
    pub(crate) log: Log,
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct RecoveryMessage {
    pub(crate) replica_number: ReplicaId,
    pub(crate) nonce: u64,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct RecoveryResponseMessage {
    pub(crate) replica_number: ReplicaId,
    pub(crate) view: ReplicaId,
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
    pub(crate) view: ReplicaId,
    pub(crate) request_number: usize,
    pub(crate) result: Operation,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct Operation {
    pub(crate) content: Bytes,
}

impl From<Bytes> for Operation {
    fn from(value: Bytes) -> Self {
        Operation { content: value }
    }
}

impl Encode for Operation {
    fn encode<E: bincode::enc::Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        bincode::Encode::encode(self.content.as_ref(), encoder)
    }
}

impl<Context> Decode<Context> for Operation {
    fn decode<D: Decoder<Context = Context>>(decoder: &mut D) -> Result<Self, DecodeError> {
        let bytes: Vec<u8> = Decode::decode(decoder)?;
        Ok(Operation::from(Bytes::from(bytes)))
    }
}

impl<'de, Context> BorrowDecode<'de, Context> for Operation {
    fn borrow_decode<D: BorrowDecoder<'de, Context = Context>>(
        decoder: &mut D,
    ) -> Result<Self, DecodeError> {
        let bytes: &[u8] = bincode::BorrowDecode::borrow_decode(decoder)?;
        Ok(Operation::from(Bytes::copy_from_slice(bytes)))
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};

    use crate::{
        Operation,
        message::{Message, RequestMessage},
    };

    #[test]
    fn test_encode_decode_single_message() {
        // given
        let message = Message::Request(RequestMessage {
            view: 0,
            request_number: 0,
            client_id: 0,
            operation: Operation::from(Bytes::from(vec![1, 2, 3])),
        });
        let config = bincode::config::standard();

        // when
        let encoded = message.encode(config).unwrap();
        let mut encoded = BytesMut::from(encoded);
        let decoded = Message::decode(&mut encoded, config).unwrap();

        // then
        assert_eq!(
            decoded,
            Some(Message::Request(RequestMessage {
                view: 0,
                request_number: 0,
                client_id: 0,
                operation: Operation::from(Bytes::from(vec![1, 2, 3])),
            }))
        );
    }
}
