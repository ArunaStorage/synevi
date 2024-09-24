use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Clone)]
pub struct Message {
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub src: String,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub dest: String,
    pub body: Body,
}

impl Message {
    pub fn reply(&self, mut body: Body) -> Message {
        body.in_reply_to = self.body.msg_id;
        Message {
            src: self.dest.clone(),
            dest: self.src.clone(),
            body,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Clone)]
pub struct Body {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<u64>,
    #[serde(flatten)]
    pub msg_type: MessageType,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum MessageType {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
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
        value: u64,
    },
    Write {
        key: u64,
        value: u64,
    },
    WriteOk,
    Cas {
        key: u64,
        from: u64,
        to: u64,
    },
    CasOk,

    // internal consensus specific:
    PreAccept {
        id: Vec<u8>,
        event: Vec<u8>,
        t0: Vec<u8>,
        last_applied: Vec<u8>,
    },
    PreAcceptOk {
        t0: Vec<u8>,
        t: Vec<u8>,
        deps: Vec<u8>,
        nack: bool,
    },
    Accept {
        id: Vec<u8>,
        ballot: Vec<u8>,
        event: Vec<u8>,
        t0: Vec<u8>,
        t: Vec<u8>,
        deps: Vec<u8>,
        last_applied: Vec<u8>,
    },
    AcceptOk {
        t0: Vec<u8>,
        deps: Vec<u8>,
        nack: bool,
    },
    Commit {
        id: Vec<u8>,
        event: Vec<u8>,
        t0: Vec<u8>,
        t: Vec<u8>,
        deps: Vec<u8>,
    },
    CommitOk {
        t0: Vec<u8>,
    },
    Apply {
        id: Vec<u8>,
        event: Vec<u8>,
        t0: Vec<u8>,
        t: Vec<u8>,
        deps: Vec<u8>,
        transaction_hash: Vec<u8>,
        execution_hash: Vec<u8>,
    },
    ApplyOk {
        t0: Vec<u8>,
    },
    Recover {
        id: Vec<u8>,
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

impl Default for MessageType {
    fn default() -> Self {
        Self::Error {
            code: 0,
            text: "Unknown".to_string(),
        }
    }
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
                msg_id: Some(1),
                in_reply_to: None,
                msg_type: MessageType::Echo {
                    echo: "Hello world".to_string(),
                },
            },
        };
        let serialized = serde_json::from_str(echo).unwrap();
        assert_eq!(msg, serialized);
    }
    #[test]
    fn test_kv_deserialization() {
        let read = r#"{ "id":0, "src": "c1", "dest": "n1", "body": { "type":"read", "msg_id":2, "key":0 } }"#;
        let write = r#"{ "id":0, "src": "c1", "dest": "n1", "body": { "type":"write", "msg_id":2, "key":0, "value":0 } }"#;

        let read_msg = Message {
            src: "c1".to_string(),
            dest: "n1".to_string(),
            body: Body {
                msg_id: Some(2),
                in_reply_to: None,
                msg_type: MessageType::Read {
                    key: "0".to_string().parse().unwrap(),
                },
            },
        };
        let write_msg = Message {
            src: "c1".to_string(),
            dest: "n1".to_string(),
            body: Body {
                msg_id: Some(2),
                in_reply_to: None,
                msg_type: MessageType::Write {
                    key: "0".to_string().parse().unwrap(),
                    value: 0,
                },
            },
        };
        let read_serialized = serde_json::from_str(read).unwrap();
        assert_eq!(read_msg, read_serialized);
        let write_serialized = serde_json::from_str(write).unwrap();
        assert_eq!(write_msg, write_serialized);
    }

    #[test]
    fn test_consensus_deserialization() {
        let mut pre_accept = r#"{ "src": "n1", "dest": "n3", "body" : { "type": "pre_accept", "id": [1, 144, 198, 75, 86, 76, 47, 9, 101, 85, 107, 197, 203, 4, 251, 45], "event": [0, 48], "t0": [0, 0, 23, 227, 85, 187, 13, 137, 207, 83, 0, 1, 0, 49, 0, 0], "last_applied":  [1, 144, 198, 75, 86, 76, 47, 9, 101, 85, 107, 197, 203, 4, 251, 45] } }"#.to_string();
        let mut pre_accept_ok = r#"{ "src": "n3", "dest": "n1", "body": { "type": "pre_accept_ok",  "t0": [0, 0, 23, 227, 85, 187, 13, 137, 207, 83, 0, 1, 0, 49, 0, 0], "t": [0, 0, 23, 227, 85, 187, 13, 137, 207, 83, 0, 1, 0, 49, 0, 0], "deps": [], "nack": false  } }"#.to_string();

        let pre_accept_msg = Message {
            src: "n1".to_string(),
            dest: "n3".to_string(),
            body: Body {
                msg_id: None,
                in_reply_to: None,
                msg_type: MessageType::PreAccept {
                    id: vec![
                        1, 144, 198, 75, 86, 76, 47, 9, 101, 85, 107, 197, 203, 4, 251, 45,
                    ],
                    event: vec![0, 48],
                    t0: vec![0, 0, 23, 227, 85, 187, 13, 137, 207, 83, 0, 1, 0, 49, 0, 0],
                    last_applied: vec![
                        1, 144, 198, 75, 86, 76, 47, 9, 101, 85, 107, 197, 203, 4, 251, 45,
                    ],
                },
            },
        };
        let pre_accept_ok_msg = Message {
            src: "n3".to_string(),
            dest: "n1".to_string(),
            body: Body {
                msg_id: None,
                in_reply_to: None,
                msg_type: MessageType::PreAcceptOk {
                    t0: vec![0, 0, 23, 227, 85, 187, 13, 137, 207, 83, 0, 1, 0, 49, 0, 0],
                    t: vec![0, 0, 23, 227, 85, 187, 13, 137, 207, 83, 0, 1, 0, 49, 0, 0],
                    deps: vec![],
                    nack: false,
                },
            },
        };
        let pre_accept_serialized: Message = serde_json::from_str(&pre_accept).unwrap();
        assert_eq!(pre_accept_serialized, pre_accept_msg);

        let mut pre_accept_deserialized: String = serde_json::to_string(&pre_accept_msg).unwrap();
        pre_accept_deserialized.retain(|char| char != ' ');
        pre_accept.retain(|char| char != ' ');
        assert_eq!(pre_accept_deserialized, pre_accept);

        let pre_accept_ok_serialized: Message = serde_json::from_str(&pre_accept_ok).unwrap();
        assert_eq!(pre_accept_ok_serialized, pre_accept_ok_msg);

        let mut pre_accept_ok_deserialized: String =
            serde_json::to_string(&pre_accept_ok_msg).unwrap();
        pre_accept_ok_deserialized.retain(|char| char != ' ');
        pre_accept_ok.retain(|char| char != ' ');
        assert_eq!(pre_accept_ok_deserialized, pre_accept_ok);
    }
}
