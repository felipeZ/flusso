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
use serde_json::Value;
use std::time::Duration;

#[derive(Debug)]
pub struct WebsocketConsumer {
    pub url: String,
    pub heartbeat_msg: String,
    pub subscription_message: String,
}

impl WebsocketConsumer {
    pub fn new<T: ToOwned<Owned = String>>(
        url: T,
        heartbeat_msg: T,
        subscription_message: T,
    ) -> Self {
        let url = url.to_owned();
        let heartbeat_msg = heartbeat_msg.to_owned();
        let subscription_message = subscription_message.to_owned();
        Self {
            url,
            heartbeat_msg,
            subscription_message,
        }
    }
    pub async fn consume(
        &self,
        channel_sender: async_channel::Sender<Value>,
    ) -> utils::GenericResult<()> {
        let (websocket, _) = connect_async(&self.url).await.unwrap();
        let (mut websocket_sender, websocket_receiver) = websocket.split();

        Self::subscribe(&mut websocket_sender, self.subscription_message.clone()).await?;
        try_join!(
            task::spawn(Self::listen(websocket_receiver, channel_sender)),
            self.send_heartbeat(websocket_sender)
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
        &self,
        mut sender: SplitSink<WebSocketStream<ConnectStream>, Message>,
    ) -> utils::GenericResult<()> {
        loop {
            let msg = self.heartbeat_msg.clone();
            sender.send(Message::Text(msg)).await?;
            task::sleep(Duration::from_secs(30)).await;
        }
    }
}
