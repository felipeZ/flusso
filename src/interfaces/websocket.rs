/*!

## Websocket consumer


*/

use async_channel;
use async_tungstenite::async_std::{connect_async, ConnectStream};
use async_tungstenite::tungstenite::protocol::Message;
use async_tungstenite::WebSocketStream;

use crate::pipe::Pipe;
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
    pub fn new(url: String, heartbeat_msg: String, subscription_message: String) -> Self {
        Self {
            url,
            heartbeat_msg,
            subscription_message,
        }
    }
    pub async fn consume<P: 'static + Pipe>(
        &self,
        channel_sender: async_channel::Sender<Value>,
        ops: P,
    ) -> utils::GenericResult<()> {
        let (websocket, _) = connect_async(&self.url).await.unwrap();
        let (mut websocket_sender, websocket_receiver) = websocket.split();

        Self::subscribe(&mut websocket_sender, self.subscription_message.clone()).await?;
        try_join!(
            task::spawn(Self::listen(websocket_receiver, channel_sender, ops)),
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

    async fn listen<P: 'static + Pipe>(
        mut receiver: SplitStream<WebSocketStream<ConnectStream>>,
        mut channel_sender: async_channel::Sender<Value>,
        ops: P,
    ) -> utils::GenericResult<()> {
        while let Some(msg) = receiver.next().await {
            let event = msg.unwrap().into_text().unwrap();
            Self::send_data_to_channel(&event, &mut channel_sender, &ops).await?;
        }
        Ok(())
    }

    async fn send_data_to_channel<P: 'static + Pipe>(
        event: &str,
        channel_sender: &mut async_channel::Sender<Value>,
        ops: &P,
    ) -> utils::GenericResult<()> {
        let dict: Value = serde_json::from_str(event)?;
        if ops.filter(&dict) {
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
