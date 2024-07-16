use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Clone)]
#[serde(rename_all = "snake_case")]
pub enum MessageType {
    Init,
    InitOk,
    #[default]
    Error,
    Echo,   // For testing
    EchoOk, // For testing
    Read,
    ReadOk,
    Write,
    WriteOk,
    // internal consensus specific:
    PreAccept,
    PreAcceptOk,
    Commit,
    CommitOk,
    Accept,
    AcceptOk,
    Apply,
    ApplyOk,
    Recover,
    RecoverOk,
    Cas,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Clone)]
pub struct Message {
    #[serde(skip_serializing_if = "u64_is_zero", default)]
    pub id: u64,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub src: String,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub dest: String,
    pub body: Body,
}

fn u64_is_zero(num: &u64) -> bool {
    *num == 0
}

impl Message {
    pub fn reply(&self, mut body: Body) -> Message {
        body.in_reply_to = self.body.msg_id;
        Message {
            id: self.id + 1,
            src: self.dest.clone(),
            dest: self.src.clone(),
            body,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Clone)]
pub struct Body {
    #[serde(rename = "type")]
    pub msg_type: MessageType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<u64>,
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub additional_fields: Option<AdditionalFields>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
#[serde(untagged)]
pub enum AdditionalFields {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    Error {
        code: u64,
        text: String,
    },
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
    Read {
        key: u64,
    },
    ReadOk {
        key: u64,
        value: String,
    },
    Write {
        key: u64,
        value: String,
    },

    // internal consensus specific:
    PreAccept {
        event: Vec<u8>,
        t0: Vec<u8>,
    },
    PreAcceptOk {
        t0: Vec<u8>,
        t: Vec<u8>,
        deps: Vec<u8>,
        nack: bool,
    },
    Accept {
        ballot: Vec<u8>,
        event: Vec<u8>,
        t0: Vec<u8>,
        t: Vec<u8>,
        deps: Vec<u8>,
    },
    AcceptOk {
        t0: Vec<u8>,
        deps: Vec<u8>,
        nack: bool,
    },
    Commit {
        event: Vec<u8>,
        t0: Vec<u8>,
        t: Vec<u8>,
        deps: Vec<u8>,
    },
    CommitOk {
        t0: Vec<u8>,
    },
    Apply {
        event: Vec<u8>,
        t0: Vec<u8>,
        t: Vec<u8>,
        deps: Vec<u8>,
    },
    ApplyOk {
        t0: Vec<u8>,
    },
    Recover {
        ballot: Vec<u8>,
        event: Vec<u8>,
        t0: Vec<u8>,
    },
    RecoverOk {
        t0: Vec<u8>,
        local_state: i32,
        wait: Vec<u8>,
        superseding: bool,
        deps: Vec<u8>,
        t: Vec<u8>,
        nack: Vec<u8>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_deserialization() {
        let echo = r#"{ "src": "c1", "dest": "n1", "body": { "type": "echo", "msg_id": 1, "echo": "Hello world" }}"#;

        let msg = Message {
            src: "c1".to_string(),
            dest: "n1".to_string(),
            body: Body {
                msg_type: MessageType::Echo,
                msg_id: Some(1),
                in_reply_to: None,
                additional_fields: Some(AdditionalFields::Echo {
                    echo: "Hello world".to_string(),
                }),
            },
            ..Default::default()
        };
        let serialized = serde_json::from_str(echo).unwrap();
        assert_eq!(msg, serialized);
    }
    #[test]
    fn test_kv_deserde() {
        let echo = r#"{ "id":0, "src": "c1", "dest": "n1", "body": { "type":"read", "msg_id":2, "key": "0" } }"#;

        let msg = Message {
            src: "c1".to_string(),
            dest: "n1".to_string(),
            body: Body {
                msg_type: MessageType::Read,
                msg_id: Some(2),
                in_reply_to: None,
                additional_fields: Some(AdditionalFields::Read {
                    key: "0".to_string(),
                }),
            },
            ..Default::default()
        };
        let serialized = serde_json::from_str(echo).unwrap();
        assert_eq!(msg, serialized);
    }
}
