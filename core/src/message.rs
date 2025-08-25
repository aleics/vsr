use std::io::Cursor;

use bincode::{
    BorrowDecode, Decode, Encode,
    config::Configuration,
    de::{BorrowDecoder, Decoder},
    error::{DecodeError, EncodeError},
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

use crate::{ClientId, ReplicaId, checksum::checksum, replica::Log};

/// BundledHeader represents the header of a bundled message. It's used
/// to verify the integrity of the message.
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct BundledHeader {
    pub(crate) checksum: u128,
}

impl BundledHeader {
    /// Creates a new BundledHeader for a given message.
    fn for_message(message: &[u8]) -> Self {
        BundledHeader {
            checksum: checksum(message),
        }
    }

    /// Verifies the checksum of the message by calculating the checksum of
    /// the message and comparing it to the stored checksum.
    pub fn verify_checksum(&self, message: &[u8]) -> bool {
        self.checksum == checksum(message)
    }
}

#[derive(Error, Debug)]
pub(crate) enum UnbundleError {
    #[error("Message checksum mismatch")]
    ChecksumMismatch,
    #[error(transparent)]
    DecodeError(#[from] DecodeError),
}

/// BundledMessage represents a message that has been bundled for transmission.
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct BundledMessage {
    header: BundledHeader,
    body: Vec<u8>,
}

impl BundledMessage {
    /// Bundle a message for transmission.
    pub(crate) fn bundle(
        message: &Message,
        config: Configuration,
    ) -> Result<BundledMessage, EncodeError> {
        let body = bincode::encode_to_vec(message, config)?;
        let header = BundledHeader::for_message(&body);

        Ok(BundledMessage { header, body })
    }

    /// Unbundle a message by verifying its content and decoding it.
    pub(crate) fn unbundle(&self, config: Configuration) -> Result<Message, UnbundleError> {
        if !self.header.verify_checksum(&self.body) {
            return Err(UnbundleError::ChecksumMismatch);
        }

        let (message, _) = bincode::decode_from_slice(&self.body, config)?;
        Ok(message)
    }

    /// Encode the [`BundledMessage`] in binary, by appending the message with its size
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

    /// Decode the given bytes into a [`BundledMessage`] using the framing implemented
    /// in [`BundledMessage::encode`]
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

pub(crate) const MESSAGE_SIZE_MAX: usize = 8 * 1024;
const MESSAGE_SIZE_BYTES: usize = 4;

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(crate) struct Header {
    pub(crate) view: ReplicaId,
}

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
