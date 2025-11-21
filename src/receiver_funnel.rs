use std::{cell::RefCell, collections::HashSet, process::Output, sync::Arc};

use futures::Stream;
use tokio::{
    sync::{
        Mutex,
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    },
    task::JoinHandle,
};
use tokio_stream::StreamExt;

struct Subscription {
    pub receiver_id: u32,
}

pub enum Control<T, S>
where
    T: Unpin + Clone + Send + 'static,
    S: Stream<Item = T> + Unpin + Send + 'static,
{
    Add { id: u32, receiver: S },
    Rmv(u32),
}

#[derive(Debug, Clone)]
pub struct ReceiverFunnel<T, S>
where
    T: Unpin + Clone + Send + 'static,
    S: Stream<Item = T> + Unpin + Send + 'static,
{
    used: RefCell<HashSet<u32>>,
    sub_tx: Option<UnboundedSender<Control<T, S>>>,
}

impl<T, S> ReceiverFunnel<T, S>
where
    T: Unpin + Clone + Send + 'static,
    S: Stream<Item = T> + Unpin + Send + 'static,
{
    pub fn start() -> (Self, UnboundedReceiver<T>) {
        let subscriptions = Arc::new(Mutex::new(Vec::<Subscription>::new()));
        let (new_sub_tx, mut new_sub_rx) = unbounded_channel::<Control<T, S>>();
        let (funnel_tx, funnel_rx) = unbounded_channel::<T>();

        let subscriptions_clone = Arc::clone(&subscriptions);

        tokio::spawn(async move {
            let mut streams = tokio_stream::StreamMap::<u32, S>::new();
            let mut active_ids = HashSet::<u32>::new();

            loop {
                tokio::select! {
                    Some(control) = new_sub_rx.recv() => {
                        match control {
                            Control::Add { id, receiver, } => {
                                streams.insert(id, receiver);
                                active_ids.insert(id);

                                let mut subs = subscriptions_clone.lock().await;
                                subs.push(Subscription {
                                    receiver_id: id,
                                });
                            }
                            Control::Rmv(id) => {
                                streams.remove(&id);
                                active_ids.remove(&id);

                                let mut subs = subscriptions_clone.lock().await;
                                subs.retain(|s| s.receiver_id != id);
                            }
                        }
                    }

                    Some((id, item)) = streams.next() => {
                        if active_ids.contains(&id) {
                            let _ = funnel_tx.send(item);
                        }
                    }
                    else => break,
                }
            }
        });

        (
            ReceiverFunnel {
                sub_tx: Some(new_sub_tx),
                used: RefCell::new(HashSet::new()),
            },
            funnel_rx,
        )
    }

    pub fn add_subscription(&self, receiver: S) -> Option<u32> {
        if let Some(sub_tx) = self.sub_tx.as_ref() {
            let mut id = 0_u32;
            while self.used.borrow().contains(&id) {
                if let Some(n) = id.checked_add(1_u32) {
                    id = n
                } else {
                    return None;
                }
            }

            self.used.borrow_mut().insert(id);

            let control = Control::Add { id, receiver };
            let _ = sub_tx.send(control);
            return Some(id);
        }
        None
    }

    pub fn remove_subscription(&self, id: u32) {
        if let Some(sub_tx) = self.sub_tx.as_ref() {
            let control = Control::Rmv(id);
            let _ = sub_tx.send(control);
            self.used.borrow_mut().remove(&id);
        }
    }
}
