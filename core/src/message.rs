use std::io::Cursor;

use bincode::{
    BorrowDecode, Decode, Encode,
    config::Configuration,
    de::{BorrowDecoder, Decoder},
    error::{DecodeError, EncodeError},
};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{ClientId, ReplicaId, replica::Log};

pub(crate) const MESSAGE_SIZE_MAX: usize = 8 * 1024;
const MESSAGE_SIZE_BYTES: usize = 4;

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

        let message_size = payload.len() + MESSAGE_SIZE_BYTES;
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
    ) -> Result<Option<Self>, DecodeError> {
        if buf.len() < MESSAGE_SIZE_BYTES {
            return Ok(None);
        }

        // Read the first 4 bytes (`u32`) containing the size of the message
        let mut cursor = Cursor::new(&buf[..MESSAGE_SIZE_BYTES]);
        let length = cursor.get_u32() as usize;

        if buf.len() < MESSAGE_SIZE_BYTES + length {
            return Ok(None);
        }

        // Decode the message's payload
        buf.advance(MESSAGE_SIZE_BYTES);
        let payload = buf.split_to(length);

        let (message, _) = bincode::decode_from_slice(&payload.freeze(), config)?;
        Ok(Some(message))
    }
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct RequestMessage {
    pub(crate) header: Header,
    pub(crate) body: RequestMessageBody,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct RequestMessageBody {
    pub(crate) request_number: u32,
    pub(crate) client_id: ClientId,
    pub(crate) operation: Operation,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct PrepareMessage {
    pub(crate) header: Header,
    pub(crate) body: PrepareMessageBody,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct PrepareMessageBody {
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
    pub(crate) request: RequestMessage,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct PrepareOkMessage {
    pub(crate) header: Header,
    pub(crate) body: PrepareOkMessageBody,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct PrepareOkMessageBody {
    pub(crate) operation_number: usize,
    pub(crate) replica: ReplicaId,
    pub(crate) client_id: ClientId,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct CommitMessage {
    pub(crate) header: Header,
    pub(crate) body: CommitMessageBody,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct CommitMessageBody {
    pub(crate) replica: ReplicaId,
    pub(crate) operation_number: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct GetStateMessage {
    pub(crate) header: Header,
    pub(crate) body: GetStateMessageBody,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct GetStateMessageBody {
    pub(crate) operation_number: usize,
    pub(crate) replica: ReplicaId,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct NewStateMessage {
    pub(crate) header: Header,
    pub(crate) body: NewStateMessageBody,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct NewStateMessageBody {
    pub(crate) replica: ReplicaId,
    pub(crate) log_after_operation: Log,
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct StartViewChangeMessage {
    pub(crate) header: Header,
    pub(crate) body: StartViewChangeMessageBody,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct StartViewChangeMessageBody {
    pub(crate) new_view: ReplicaId,
    pub(crate) replica: ReplicaId,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct DoViewChangeMessage {
    pub(crate) header: Header,
    pub(crate) body: DoViewChangeMessageBody,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct DoViewChangeMessageBody {
    pub(crate) old_view: ReplicaId,
    pub(crate) new_view: ReplicaId,
    pub(crate) log: Log,
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
    pub(crate) replica: ReplicaId,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct StartViewMessage {
    pub(crate) header: Header,
    pub(crate) body: StartViewMessageBody,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct StartViewMessageBody {
    pub(crate) replica: ReplicaId,
    pub(crate) log: Log,
    pub(crate) operation_number: usize,
    pub(crate) commit_number: usize,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct RecoveryMessage {
    pub(crate) header: Header,
    pub(crate) body: RecoveryMessageBody,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct RecoveryMessageBody {
    pub(crate) replica: ReplicaId,
    pub(crate) nonce: u64,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct RecoveryResponseMessage {
    pub(crate) header: Header,
    pub(crate) body: RecoveryResponseMessageBody,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct RecoveryResponseMessageBody {
    pub(crate) replica: ReplicaId,
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
    pub(crate) header: Header,
    pub(crate) body: ReplyMessageBody,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct ReplyMessageBody {
    pub(crate) request_number: u32,
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
        message::{BundledMessage, Header, Message, RequestMessage, RequestMessageBody},
    };

    #[test]
    fn test_encode_decode_single_message() {
        // given
        let message = Message::Request(RequestMessage {
            header: Header { view: 0 },
            body: RequestMessageBody {
                request_number: 0,
                client_id: 0,
                operation: Operation::from(Bytes::from(vec![1, 2, 3])),
            },
        });
        let config = bincode::config::standard();

        // when
        let bundled = BundledMessage::bundle(&message, config).unwrap();
        let encoded = bundled.encode(config).unwrap();
        let mut encoded = BytesMut::from(encoded);
        let decoded = BundledMessage::decode(&mut encoded, config)
            .unwrap()
            .unwrap();
        let unbundled = decoded.unbundle(config).unwrap();

        // then
        assert_eq!(
            unbundled,
            Message::Request(RequestMessage {
                header: Header { view: 0 },
                body: RequestMessageBody {
                    request_number: 0,
                    client_id: 0,
                    operation: Operation::from(Bytes::from(vec![1, 2, 3]))
                },
            })
        );
    }
}
