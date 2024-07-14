use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
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
}

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
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
            id: self.id.clone() + 1,
            src: self.dest.clone(),
            dest: self.src.clone(),
            body,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
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

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[serde(untagged)]
pub enum AdditionalFields {
    Init { node_id: String, nodes: Vec<String> },
    Error { code: u64, text: String },
    Echo { echo: String },
    EchoOk { echo: String },
    Read { key: String },
    ReadOk { key: String, value: String },
    Write { key: String, value: String },
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
        let serialized = serde_json::from_str(&echo).unwrap();
        assert_eq!(msg, serialized);
    }
}
