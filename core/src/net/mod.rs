use super::*;
use actors::Transport;
use arc_swap::ArcSwap;
use dispatch::lookup::ActorStore;
use futures::{self, stream::Stream, Future};
use net::events::{NetworkError, NetworkEvent};

use std::{net::SocketAddr, sync::Arc, thread};

use crate::{
    messaging::SerializedFrame,
    net::{events::DispatchEvent, frames::*, network_thread::NetworkThread},
};
use bytes::{Buf, BufMut, BytesMut};
use mio::{Ready, Registration, SetReadiness};
use std::sync::mpsc::{channel, Receiver as Recv, Sender};

#[allow(missing_docs)]
pub mod buffer;
pub(crate) mod buffer_pool;
pub mod frames;
pub(crate) mod network_channel;
pub(crate) mod network_thread;

/// The state of a connection
#[derive(Debug)]
pub enum ConnectionState {
    /// Newly created
    New,
    /// Still initialising
    Initializing,
    /// Connected with a confirmed canonical SocketAddr
    Connected(SocketAddr),
    /// Already closed
    Closed,
    /// Threw an error
    Error(std::io::Error),
}

/// Events on the network level
pub mod events {
    use std;

    use super::ConnectionState;
    use crate::net::frames::*;
    use std::net::SocketAddr;

    use crate::messaging::SerializedFrame;

    /// Network events emitted by the network `Bridge`
    #[derive(Debug)]
    pub enum NetworkEvent {
        /// The state of a connection changed
        Connection(SocketAddr, ConnectionState),
        /// Data was received
        Data(Frame),
    }

    /// BridgeEvents emitted to the network `Bridge`
    #[derive(Debug)]
    pub enum DispatchEvent {
        /// Send the SerializedFrame to receiver associated with the SocketAddr
        Send(SocketAddr, SerializedFrame),
        /// Tells the network thread to Stop
        Stop(),
        /// Tells the network adress to open up a channel to the SocketAddr
        Connect(SocketAddr),
    }

    /// Errors emitted byt the network `Bridge`
    #[derive(Debug)]
    pub enum NetworkError {
        /// The protocol is not supported in this implementation
        UnsupportedProtocol,
        /// There is no executor to run the bridge on
        MissingExecutor,
        /// Some other IO error
        Io(std::io::Error),
    }

    impl From<std::io::Error> for NetworkError {
        fn from(other: std::io::Error) -> Self {
            NetworkError::Io(other)
        }
    }
}

/// The configuration for the network `Bridge`
pub struct BridgeConfig {
    retry_strategy: RetryStrategy,
}

impl BridgeConfig {
    /// Create a new config
    ///
    /// This is the same as the [Default](std::default::Default) implementation.
    pub fn new() -> Self {
        BridgeConfig::default()
    }
}

enum RetryStrategy {
    ExponentialBackoff { base_ms: u64, num_tries: usize },
}

impl Default for BridgeConfig {
    fn default() -> Self {
        let retry_strategy = RetryStrategy::ExponentialBackoff {
            base_ms: 100,
            num_tries: 5,
        };
        BridgeConfig { retry_strategy }
    }
}

/// Bridge to Tokio land, responsible for network connection management
pub struct Bridge {
    /// Network-specific configuration
    cfg: BridgeConfig,
    /// Executor belonging to the Tokio runtime
    // pub(crate) executor: Option<TaskExecutor>,
    /// Queue of network events emitted by the network layer
    // events: sync::mpsc::UnboundedSender<NetworkEvent>,
    /// Core logger; shared with network thread
    log: KompactLogger,
    /// Shared actor reference lookup table
    lookup: Arc<ArcSwap<ActorStore>>,
    /// Network Thread stuff:
    // network_thread: Box<NetworkThread>,
    // ^ Can we avoid storing this by moving it into itself?
    network_input_queue: Sender<events::DispatchEvent>,
    network_thread_registration: SetReadiness,
    /// Tokio Runtime
    // tokio_runtime: Option<Runtime>,
    /// Reference back to the Kompact dispatcher
    dispatcher: Option<DispatcherRef>,
    /// Socket the network actually bound on
    bound_addr: Option<SocketAddr>,
    network_thread_receiver: Box<Recv<bool>>,
}

// impl bridge
impl Bridge {
    /// Creates a new bridge
    ///
    /// # Returns
    /// A tuple consisting of the new Bridge object and the network event receiver.
    /// The receiver will allow responding to [NetworkEvent]s for external state management.
    pub fn new(
        lookup: Arc<ArcSwap<ActorStore>>,
        network_thread_log: KompactLogger,
        bridge_log: KompactLogger,
        addr: SocketAddr,
        dispatcher_ref: DispatcherRef,
    ) -> (Self, SocketAddr) {
        let (registration, network_thread_registration) = Registration::new2();
        let (sender, receiver) = channel();
        let (network_thread_sender, network_thread_receiver) = channel();
        let mut network_thread = NetworkThread::new(
            network_thread_log,
            addr,
            lookup.clone(),
            registration,
            network_thread_registration.clone(),
            receiver,
            network_thread_sender,
            dispatcher_ref.clone(),
        );
        let bound_addr = network_thread.addr.clone();
        let bridge = Bridge {
            cfg: BridgeConfig::default(),
            log: bridge_log,
            lookup,
            //network_thread: Box::new(network_thread),
            network_input_queue: sender,
            network_thread_registration,
            dispatcher: Some(dispatcher_ref),
            bound_addr: Some(bound_addr.clone()),
            network_thread_receiver: Box::new(network_thread_receiver),
        };
        thread::Builder::new()
            .name("network_thread".to_string())
            .spawn(move || {
                network_thread.run();
            });
        (bridge, bound_addr)
    }

    /// Sets the dispatcher reference, returning the previously stored one
    pub fn set_dispatcher(&mut self, dispatcher: DispatcherRef) -> Option<DispatcherRef> {
        std::mem::replace(&mut self.dispatcher, Some(dispatcher))
    }

    /// Stops the bridge
    pub fn stop(self) -> Result<(), NetworkBridgeErr> {
        debug!(self.log, "Stopping NetworkBridge...");
        self.network_input_queue.send(DispatchEvent::Stop());
        self.network_thread_registration
            .set_readiness(Ready::readable());
        self.network_thread_receiver.recv(); // should block until something is sent
        debug!(self.log, "Stopped NetworkBridge.");
        Ok(())
    }

    /// Returns the local address if already bound
    pub fn local_addr(&self) -> &Option<SocketAddr> {
        &self.bound_addr
    }

    /// Forwards `serialized` to the NetworkThread and makes sure that it will wake up.
    pub fn route(&self, addr: SocketAddr, serialized: SerializedFrame) -> () {
        match serialized {
            SerializedFrame::Bytes(bytes) => {
                let size = FrameHead::encoded_len() + bytes.len();
                let mut buf = BytesMut::with_capacity(size);
                let head = FrameHead::new(FrameType::Data, bytes.len());
                head.encode_into(&mut buf);
                buf.put_slice(bytes.bytes());
                self.network_input_queue.send(events::DispatchEvent::Send(
                    addr,
                    SerializedFrame::Bytes(buf.freeze()),
                ));
            }
            SerializedFrame::Chunk(chunk) => {
                self.network_input_queue.send(events::DispatchEvent::Send(
                    addr,
                    SerializedFrame::Chunk(chunk),
                ));
            }
        }
        self.network_thread_registration
            .set_readiness(Ready::readable());
    }

    /// Attempts to establish a TCP connection to the provided `addr`.
    ///
    /// # Side effects
    /// When the connection is successul:
    ///     - a `ConnectionState::Connected` is dispatched on the network bridge event queue
    //     - a new task is spawned on the Tokio runtime for driving the TCP connection's  I/O
    ///
    /// # Errors
    /// If the provided protocol is not supported or if the Tokio runtime's executor has not been
    /// set.
    pub fn connect(&mut self, proto: Transport, addr: SocketAddr) -> Result<(), NetworkError> {
        //println!("bridge bound to {} connecting to addr {}, ", self.bound_addr.unwrap(), addr);
        match proto {
            Transport::TCP => {
                self.network_input_queue
                    .send(events::DispatchEvent::Connect(addr));
                self.network_thread_registration
                    .set_readiness(Ready::readable());
                Ok(())
            }
            _other => Err(NetworkError::UnsupportedProtocol),
        }
    }
}

/// Errors which the NetworkBridge might return, not used for now.
#[derive(Debug)]
pub enum NetworkBridgeErr {
    /// Something went wrong while binding
    Binding(String),
    /// Something went wrong with the thread
    Thread(String),
    /// Something else went wrong
    Other(String),
}
