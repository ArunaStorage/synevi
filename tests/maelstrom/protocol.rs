use crate::messages::Message;
use serde_json::Deserializer;
use tokio::sync::mpsc::{Receiver, Sender};

pub struct MessageHandler;

type StdOutSender = Sender<Message>;
type StdInReceiver = Receiver<Message>;

impl MessageHandler {
    pub fn new() -> (StdInReceiver, StdOutSender) {
        let (output_sender, mut output_receiver) = tokio::sync::mpsc::channel(10);
        let (input_sender, input_receiver) = tokio::sync::mpsc::channel(10);

        tokio::task::spawn_blocking(move || {
            let mut stream_deserializer =
                Deserializer::from_reader(std::io::stdin().lock()).into_iter::<Message>();
            while let Some(Ok(message)) = stream_deserializer.next() {
                input_sender.blocking_send(message).unwrap();
            }
        });

        tokio::task::spawn_blocking(move || {
            let mut io_lock = std::io::stdout().lock();
            while let Some(message) = output_receiver.blocking_recv() {
                serde_json::to_writer(&mut io_lock, &message).unwrap();
            }
        });

        (input_receiver, output_sender)
    }
}
