/*!

## Websocket consumer


*/

use async_channel;
use async_tungstenite::async_std::{connect_async, ConnectStream};
use async_tungstenite::tungstenite::protocol::Message;
use async_tungstenite::WebSocketStream;

use crate::utils;
use async_std::task;
use futures::stream::{SplitSink, SplitStream};
use futures::{try_join, SinkExt, StreamExt};
use serde_json::{json, Value};
use std::time::Duration;

#[derive(Debug)]
pub struct WebsocketConsumer {
    pub url: String,
}

impl WebsocketConsumer {
    pub fn new(url: String) -> Self {
        Self { url }
    }
    pub async fn consume(
        &self,
        channel_sender: async_channel::Sender<Value>,
    ) -> utils::GenericResult<()> {
        let (websocket, _) = connect_async(&self.url).await.unwrap();
        let (mut websocket_sender, websocket_receiver) = websocket.split();
        let msg_subscription = self.create_message_fro_subscription();
        Self::subscribe(&mut websocket_sender, msg_subscription).await?;
        try_join!(
            task::spawn(Self::listen(websocket_receiver, channel_sender)),
            Self::send_heartbeat(websocket_sender)
        )?;
        Ok(())
    }

    async fn subscribe(
        sender: &mut SplitSink<WebSocketStream<ConnectStream>, Message>,
        msg: String,
    ) -> utils::GenericResult<()> {
        sender.send(Message::Text(msg)).await?;
        Ok(())
    }

    async fn listen(
        mut receiver: SplitStream<WebSocketStream<ConnectStream>>,
        mut channel_sender: async_channel::Sender<Value>,
    ) -> utils::GenericResult<()> {
        while let Some(msg) = receiver.next().await {
            let event = msg.unwrap().into_text().unwrap();
            Self::send_data_to_channel(&event, &mut channel_sender).await?;
        }
        Ok(())
    }

    async fn send_data_to_channel(
        event: &str,
        channel_sender: &mut async_channel::Sender<Value>,
    ) -> utils::GenericResult<()> {
        let dict: Value = serde_json::from_str(event)?;
        if dict.get("key").is_some() {
            channel_sender.send(dict).await?
        };
        Ok(())
    }

    async fn send_heartbeat(
        mut sender: SplitSink<WebSocketStream<ConnectStream>, Message>,
    ) -> utils::GenericResult<()> {
        let event = json!({
            "event":"subscribe",
            "feed":"heartbeat"
        })
        .to_string();
        loop {
            let msg = event.clone();
            sender.send(Message::Text(msg)).await?;
            task::sleep(Duration::from_secs(30)).await;
        }
    }
    fn create_message_fro_subscription(&self) -> String {
        json!(
        {
            "event": "subscribe"
        })
        .to_string()
    }
}
