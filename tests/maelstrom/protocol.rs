use std::io::Write;

use crate::messages::Message;
use serde_json::Deserializer;

pub struct MessageHandler;

type StdOutSender = async_channel::Sender<Message>;
type StdInReceiver = async_channel::Receiver<Message>;

impl MessageHandler {
    pub fn spawn_handler() -> (StdInReceiver, StdOutSender) {
        let (output_sender, output_receiver) = async_channel::bounded::<Message>(10);
        let (input_sender, input_receiver) = async_channel::bounded(10);

        tokio::task::spawn_blocking(move || {
            let mut stream_deserializer =
                Deserializer::from_reader(std::io::stdin().lock()).into_iter::<Message>();
            while let Some(Ok(message)) = stream_deserializer.next() {
                eprintln!("Received message: {:?}", &message);
                if let Err(e) = input_sender.send_blocking(message) {
                    eprintln!("Error sending message: {:?}", e);
                    panic!();
                };
            }
        });

        tokio::task::spawn_blocking(move || {
            let mut io_lock = std::io::stdout().lock();
            let mut counter = 0;
            while let Ok(mut message) = output_receiver.recv_blocking() {
                message.body.msg_id = Some(counter);
                eprintln!("Send message: {:?}", message);
                serde_json::to_writer(&mut io_lock, &message).unwrap();
                io_lock.write_all(b"\n").unwrap();
                io_lock.flush().unwrap();
                counter += 1;
            }
        });

        (input_receiver, output_sender)
    }
}
