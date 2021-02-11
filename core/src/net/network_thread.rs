use super::*;
use crate::{
    dispatch::{
        lookup::{ActorLookup, LookupResult},
        NetworkConfig,
    },
    messaging::{DispatchEnvelope, EventEnvelope},
    net::{
        buffers::{BufferChunk, BufferPool, EncodeBuffer},
        network_channel::{ChannelState, TcpChannel},
        udp_state::UdpState,
        ConnectionState,
    },
};
use crossbeam_channel::Receiver as Recv;
use mio::{
    event::Event,
    net::{TcpListener, TcpStream, UdpSocket},
    Events,
    Poll,
    Token,
};
use rustc_hash::FxHashMap;
use serialisation::ser_helpers::deserialise_chunk_lease;
use std::{
    collections::VecDeque,
    io,
    io::{Error, ErrorKind},
    net::{Shutdown, SocketAddr},
    sync::Arc,
    time::Duration,
    usize,
};
use uuid::Uuid;
use crate::messaging::SerialisedFrame;

// Used for identifying connections
const TCP_SERVER: Token = Token(0);
const UDP_SOCKET: Token = Token(1);
// Used for identifying the dispatcher/input queue
const DISPATCHER: Token = Token(2);
const START_TOKEN: Token = Token(3);
const MAX_POLL_EVENTS: usize = 1024;
/// How many times to retry on interrupt before we give up
pub const MAX_INTERRUPTS: i32 = 9;
// We do retries when we fail to bind a socket listener during boot-up:
const MAX_BIND_RETRIES: usize = 5;
const BIND_RETRY_INTERVAL: u64 = 1000;

/// Thread structure responsible for driving the Network IO
pub struct NetworkThread {
    log: KompactLogger,
    pub addr: SocketAddr,
    lookup: Arc<ArcSwap<ActorStore>>,
    tcp_listener: Option<TcpListener>,
    udp_state: Option<UdpState>,
    poll: Poll,
    channel_map: FxHashMap<SocketAddr, TcpChannel>,
    token_map: FxHashMap<Token, SocketAddr>,
    token: Token,
    input_queue: Recv<DispatchEvent>,
    dispatcher_ref: DispatcherRef,
    buffer_pool: BufferPool,
    sent_bytes: u64,
    received_bytes: u64,
    sent_msgs: u64,
    stopped: bool,
    shutdown_promise: Option<KPromise<()>>,
    network_config: NetworkConfig,
    retry_queue: VecDeque<EventWithRetries>,
    out_of_buffers: bool,
    encode_buffer: EncodeBuffer,
}

struct EventWithRetries {
    token: Token,
    readable: bool,
    writeable: bool,
    retries: u8,
}
impl EventWithRetries {
    fn from(event: &Event) -> EventWithRetries {
        EventWithRetries {
            token: event.token(),
            readable: event.is_readable(),
            writeable: event.is_writable(),
            retries: 0,
        }
    }

    fn get_retry_event(&self) -> EventWithRetries {
        EventWithRetries {
            token: self.token,
            readable: self.readable,
            writeable: self.writeable,
            retries: self.retries + 1,
        }
    }
}

impl NetworkThread {
    /// Creates a struct for the NetworkThread and binds to a socket without actually spawning a thread.
    /// The `input_queue` is used to send DispatchEvents to the thread but they won't be read unless
    /// the `dispatcher_registration` is activated to wake up the thread.
    /// `network_thread_sender` is used to confirm shutdown of the thread.
    pub(crate) fn new(
        log: KompactLogger,
        addr: SocketAddr,
        lookup: Arc<ArcSwap<ActorStore>>,
        input_queue: Recv<DispatchEvent>,
        shutdown_promise: KPromise<()>,
        dispatcher_ref: DispatcherRef,
        network_config: NetworkConfig,
    ) -> (NetworkThread, Waker) {
        // Set-up the Listener
        debug!(
            log,
            "NetworkThread starting, trying to bind listener to address {}", &addr
        );
        match bind_with_retries(&addr, MAX_BIND_RETRIES, &log) {
            Ok(mut tcp_listener) => {
                let actual_addr = tcp_listener.local_addr().expect("could not get real addr");
                let logger = log.new(o!("addr" => format!("{}", actual_addr)));
                let mut udp_socket =
                    UdpSocket::bind(actual_addr).expect("could not bind UDP on TCP port");

                // Set up polling for the Dispatcher and the listener.
                let poll = Poll::new().expect("failed to create Poll instance in NetworkThread");

                // Register Listener
                let registry = poll.registry();
                registry
                    .register(&mut tcp_listener, TCP_SERVER, Interest::READABLE)
                    .expect("failed to register TCP SERVER");
                registry
                    .register(
                        &mut udp_socket,
                        UDP_SOCKET,
                        Interest::READABLE | Interest::WRITABLE,
                    )
                    .expect("failed to register UDP SOCKET");

                let waker = Waker::new(poll.registry(), DISPATCHER)
                    .expect("failed to create Waker for DISPATCHER");
                let mut buffer_pool = BufferPool::with_config(
                    &network_config.get_buffer_config(),
                    &network_config.get_custom_allocator(),
                );
                let encode_buffer = EncodeBuffer::with_config(
                    &network_config.get_buffer_config(),
                    &network_config.get_custom_allocator(),
                );
                let udp_buffer = buffer_pool
                    .get_buffer()
                    .expect("Could not get buffer for setting up UDP");
                let udp_state =
                    UdpState::new(udp_socket, udp_buffer, logger.clone(), &network_config);

                (
                    NetworkThread {
                        log: logger,
                        addr: actual_addr,
                        lookup,
                        tcp_listener: Some(tcp_listener),
                        udp_state: Some(udp_state),
                        poll,
                        channel_map: FxHashMap::default(),
                        token_map: FxHashMap::default(),
                        token: START_TOKEN,
                        input_queue,
                        buffer_pool,
                        sent_bytes: 0,
                        received_bytes: 0,
                        sent_msgs: 0,
                        stopped: false,
                        shutdown_promise: Some(shutdown_promise),
                        dispatcher_ref,
                        network_config,
                        retry_queue: VecDeque::new(),
                        out_of_buffers: false,
                        encode_buffer,
                    },
                    waker,
                )
            }
            Err(e) => {
                panic!(
                    "NetworkThread failed to bind to address: {:?}, addr {:?}",
                    e, &addr
                );
            }
        }
    }

    fn get_poll_timeout(&self) -> Option<Duration> {
        if self.out_of_buffers {
            Some(Duration::from_millis(
                self.network_config.get_connection_retry_interval(),
            ))
        } else if self.retry_queue.is_empty() {
            None
        } else {
            Some(Duration::from_secs(0))
        }
    }

    pub fn run(&mut self) -> () {
        trace!(self.log, "NetworkThread starting");
        let mut events = Events::with_capacity(MAX_POLL_EVENTS);
        loop {
            // Retries happen for connection interrupts and buffer-swapping
            // Performed in the main loop to avoid recursion
            self.poll
                .poll(&mut events, self.get_poll_timeout())
                .expect("Error when calling Poll");

            for event in events
                .iter()
                .map(|e| (EventWithRetries::from(e)))
                .chain(self.retry_queue.split_off(0))
            {
                self.handle_event(event);

                if self.stopped {
                    let promise = self.shutdown_promise.take().expect("shutdown promise");
                    if let Err(e) = promise.fulfil(()) {
                        error!(self.log, "Error, shutting down sender: {:?}", e);
                    };
                    trace!(self.log, "Stopped");
                    return;
                };
            }
        }
    }

    fn handle_event(&mut self, event: EventWithRetries) {
        match event.token {
            TCP_SERVER => {
                // Received an event for the TCP server socket.Accept the connection.
                if let Err(e) = self.receive_stream() {
                    error!(self.log, "Error while accepting stream {:?}", e);
                }
            }
            UDP_SOCKET => {
                /*
                if let Some(ref mut udp_state) = self.udp_state {
                    if event.writeable {
                        match udp_state.try_write() {
                            Ok(n) => {
                                self.sent_bytes += n as u64;
                            }
                            Err(e) => {
                                warn!(self.log, "Error during UDP sending: {}", e);
                            }
                        }
                    }
                    if event.readable {
                        match udp_state.try_read() {
                            Ok((n, ioret)) => {
                                if n > 0 {
                                    self.received_bytes += n as u64;
                                }
                                if IoReturn::SwapBuffer == ioret {
                                    if let Some(mut new_buffer) = self.buffer_pool.get_buffer() {
                                        udp_state.swap_buffer(&mut new_buffer);
                                        self.buffer_pool.return_buffer(new_buffer);
                                        debug!(self.log, "Swapped UDP buffer");
                                        self.out_of_buffers = false;
                                        // We do not count successful swaps in the retries, stay at the same count
                                        self.retry_queue
                                            .push_back((token, readable, writeable, retries));
                                    } else {
                                        error!(
                                            self.log,
                                            "Could not get UDP buffer, retries: {}", retries
                                        );
                                        self.out_of_buffers = true;
                                        self.retry_queue.push_back((
                                            token,
                                            readable,
                                            writeable,
                                            retries + 1,
                                        ));
                                    }
                                }
                            }
                            Err(e) => {
                                warn!(self.log, "Error during UDP reading: {}", e);
                            }
                        }
                        use dispatch::lookup::{ActorLookup, LookupResult};

                        // Forward the data frame to the correct actor
                        let lease_lookup = self.lookup.load();
                        for envelope in udp_state.incoming_messages.drain(..) {
                            match lease_lookup.get_by_actor_path(&envelope.receiver) {
                                LookupResult::Ref(actor) => {
                                    actor.enqueue(envelope);
                                }
                                LookupResult::Group(group) => {
                                    group.route(envelope, &self.log);
                                }
                                LookupResult::None => {
                                    debug!(self.log, "Could not find actor reference for destination: {:?}, dropping message", envelope.receiver);
                                }
                                LookupResult::Err(e) => {
                                    error!(
                                        self.log,
                                        "An error occurred during local actor lookup for destination: {:?}, dropping message. The error was: {}",
                                        envelope.receiver,
                                        e
                                    );
                                }
                            }
                        }
                    }
                } else {
                    debug!(self.log, "Poll triggered for removed UDP socket");
                }
                */
            }
            DISPATCHER => {
                // Message available from Dispatcher, clear the poll readiness before receiving
                self.receive_dispatch();
            }
            _ => {
                if event.writeable {
                    self.handle_writeable_event(&event);
                }
                if event.readable {
                    self.handle_readable_event(&event);
                }
            }
        }
    }

    fn handle_readable_event(&mut self, event: &EventWithRetries) {
        if let Some(addr) = self.token_map.get(&event.token) {
            if let Some(mut channel) = self.channel_map.remove(&addr) {
                loop {
                    match channel.read_frame(&mut self.buffer_pool) {
                        Ok(None) => {
                            self.channel_map.insert(*addr, channel);
                            return;
                        }
                        Ok(Some(Frame::Data(data))) => {
                            self.handle_data_frame(data);
                        }
                        Ok(Some(Frame::Start(start))) => {
                            self.handle_start(event, channel, &start);
                            return;
                        }
                        Ok(Some(Frame::Bye())) => {
                            self.handle_bye(&mut channel);
                            self.channel_map.insert(*addr, channel);
                            return;
                        }
                        Err(e) if no_buffer_space(&e) => {
                            self.out_of_buffers = true;
                            trace!(self.log, "Out of Buffers");
                            self.channel_map.insert(addr.clone(), channel);
                            self.retry_event(event);
                            return;
                        }
                        Err(e) => {
                            error!(self.log, "Error reading from channel {}: {}", &addr, &e);
                            self.channel_map.insert(*addr, channel);
                            return;
                        }
                        Ok(Some(frame)) => {
                            error!(self.log, "Unrecognized frame type {:?}", &frame);
                        }
                    }
                }
            }
        }
    }

    fn handle_writeable_event(&mut self, event: &EventWithRetries) -> () {

    }


    fn try_write(&mut self, address: &SocketAddr) {
        if let Some(mut channel) = self.channel_map.remove(address) {
            match channel.try_drain() {
                Err(ref err) if broken_pipe(err) => {
                    // Remove the channel
                    drop(channel);
                    self.lost_connection(address);
                    return;
                }
                Ok(n) => {
                    self.sent_bytes += n as u64;
                    if let ChannelState::CloseReceived(addr, id) = channel.state {
                        channel.state = ChannelState::Closed(addr, id);
                        // TODO:
                    }
                }
                Err(e) => {
                    error!(self.log, "Unhandled error while writing to {}\n{:?}", address, e);
                }
            }
        }
    }

    fn retry_event(&mut self, event: &EventWithRetries) -> () {
        if event.retries <= self.network_config.get_max_connection_retry_attempts() {
            debug!(self.log, "Retrying event, retries: {}", event.retries);
            self.retry_queue.push_back(event.get_retry_event());
        } else {
            trace!(self.log, "Event retried too many times");
            if let Some(addr) = self.token_map.remove(&event.token) {
                self.lost_connection(&addr);
            }
        }
    }

    fn handle_data_frame(&self, data: Data) -> () {
        let lease_lookup = self.lookup.load();
        let buf = data.payload();
        let envelope = deserialise_chunk_lease(buf).expect("s11n errors");
        match lease_lookup.get_by_actor_path(&envelope.receiver) {
            LookupResult::Ref(actor) => {
                actor.enqueue(envelope);
            }
            LookupResult::Group(group) => {
                group.route(envelope, &self.log);
            }
            LookupResult::None => {
                warn!(
                    self.log,
                    "Could not find actor reference for destination: {:?}, dropping message",
                    envelope.receiver
                );
            }
            LookupResult::Err(e) => {
                error!(
                    self.log,
                    "An error occurred during local actor lookup for destination: {:?}, dropping message. The error was: {}",
                    envelope.receiver,
                    e
                );
            }
        }
    }

    /// During channel initialization the threeway handshake to establish connections culminates with this function
    /// The Start(remote_addr, id) is received by the host on the receiving end of the channel initialisation.
    /// The decision is made here and now.
    /// If no other connection is registered for the remote host the decision is easy, we start the channel and send the ack.
    /// If there are other connection attempts underway there are multiple possibilities:
    ///     The other connection has not started and does not have a known UUID: it will be killed, this channel will start.
    ///     The connection has already started, in which case this channel must be killed.
    ///     The connection has a known UUID but is not connected: Use the UUID as a tie breaker for which to kill and which to keep.
    fn handle_start(&mut self, event: &EventWithRetries, mut channel: TcpChannel, start: &Start) {
        if let Some(mut other_channel) = self.channel_map.remove(&start.addr) {
            debug!(
                self.log,
                "Merging channels for remote system {}", &start.addr
            );
            if let Some(other_id) = other_channel.get_id() {
                if other_channel.connected() || other_id > start.id {
                    // The other channel should be kept and this one should be discarded.
                    self.drop_channel(channel);
                    self.channel_map.insert(start.addr, other_channel);
                    return;
                } else {
                    self.drop_channel(other_channel);
                }
            }
        }
        channel.handle_start(&start);
        channel.token = event.token;
        self.token_map.insert(event.token, start.addr);
        if let Err(e) = self.poll.registry().reregister(
            channel.stream_mut(),
            event.token,
            Interest::WRITABLE | Interest::READABLE,
        ) {
            error!(
                self.log,
                "Error when reregistering Poll for channel in handle_hello: {:?}", e
            );
        };
        self.channel_map.insert(start.addr, channel);
        self.retry_event(event);
        self.notify_connection_state(start.addr, ConnectionState::Connected);
    }

    fn drop_channel(&mut self, mut channel: TcpChannel) {
        let _ = self.poll.registry().deregister(channel.stream_mut());
        self.token_map.remove(&channel.token);
        let _ = channel.shutdown();
    }

    fn handle_ack(&mut self, addr: &SocketAddr) -> () {
        if let Some(channel) = self.channel_map.get_mut(addr) {
            debug!(self.log, "Handling ack for {}", addr);
            channel.handle_ack();
            self.notify_connection_state(*addr, ConnectionState::Connected);
        }
    }

    fn handle_bye(&self, channel: &mut TcpChannel) -> () {
        debug!(self.log, "Handling Bye for {:?}", &channel);
        if channel.handle_bye().is_ok() {
            debug!(
                self.log,
                "Connection shutdown gracefully, awaiting dispatcher Ack"
            );
            self.notify_connection_state(
                channel.get_address().expect("must have address"),
                ConnectionState::Closed,
            );
        }
    }

    fn notify_connection_state(&self, address: SocketAddr, state: ConnectionState) {
        self.dispatcher_ref
            .tell(DispatchEnvelope::Event(EventEnvelope::Network(
                NetworkEvent::Connection(address, state),
            )));
    }

    fn reject_dispatch_data(&self, address: SocketAddr, data: DispatchData) {
        self.dispatcher_ref.tell(DispatchEnvelope::Event(
            EventEnvelope::Network(NetworkEvent::RejectedData(address, data)),
        ));
    }

    fn request_stream(&mut self, addr: SocketAddr) {
        // Make sure we never request request a stream to someone we already have a connection to
        // Async communication with the dispatcher can lead to this
        if let Some(channel) = self.channel_map.remove(&addr) {
            // We already have a connection set-up
            // the connection request must have been sent before the channel was initialized
            match channel.state {
                ChannelState::Connected(_, _) => {
                    // log and inform Dispatcher to make sure it knows we're connected.
                    debug!(
                        self.log,
                        "Asked to request connection to already connected host {}", &addr
                    );
                    self.notify_connection_state(addr, ConnectionState::Connected);
                    self.channel_map.insert(addr, channel);
                    return;
                }
                ChannelState::Closed(_, _) => {
                    // We're waiting for the ClosedAck from the NetworkDispatcher
                    // This shouldn't happen but the system will likely recover from it eventually
                    debug!(
                        self.log,
                        "Requested connection to host before receiving ClosedAck {}", &addr
                    );
                    self.channel_map.insert(addr, channel);
                    return;
                }
                _ => {
                    // It was an old attempt, remove it and continue with the new request
                    drop(channel);
                }
            }
        }
        // Fetch a buffer before we make the request
        if let Some(buffer) = self.buffer_pool.get_buffer() {
            debug!(self.log, "Requesting connection to {}", &addr);
            match TcpStream::connect(addr) {
                Ok(stream) => {
                    self.store_stream(
                        stream,
                        &addr,
                        ChannelState::Requested(addr, Uuid::new_v4()),
                        buffer,
                    );
                }
                Err(e) => {
                    //  Connection will be re-requested
                    error!(
                        self.log,
                        "Failed to connect to remote host {}, error: {:?}", &addr, e
                    );
                }
            }
        } else {
            // No buffers available, Connection will be re-requested
            trace!(
                self.log,
                "No Buffers available when attempting to connect to remote host {}",
                &addr
            );
        }
    }

    #[allow(irrefutable_let_patterns)]
    fn receive_stream(&mut self) -> io::Result<()> {
        while let (stream, addr) = (self.tcp_listener.as_ref().unwrap()).accept()? {
            if let Some(buffer) = self.buffer_pool.get_buffer() {
                debug!(self.log, "Accepting connection from {}", &addr);
                self.store_stream(stream, &addr, ChannelState::Initialising, buffer);
            } else {
                // If we can't get a buffer we reject the channel immediately
                stream.shutdown(Shutdown::Both)?;
            }
        }
        Ok(())
    }

    fn store_stream(
        &mut self,
        stream: TcpStream,
        addr: &SocketAddr,
        state: ChannelState,
        buffer: BufferChunk,
    ) {
        self.token_map.insert(self.token, *addr);
        let mut channel = TcpChannel::new(
            stream,
            self.token,
            buffer,
            state,
            self.addr,
            &self.network_config,
        );
        debug!(self.log, "Saying Hello to {}", addr);
        // Whatever error is thrown here will be re-triggered and handled later.
        channel.initialise(&self.addr);
        if let Err(e) = self.poll.registry().register(
            channel.stream_mut(),
            self.token,
            Interest::READABLE | Interest::WRITABLE,
        ) {
            error!(self.log, "Failed to register polling for {}\n{:?}", addr, e);
        }
        self.channel_map.insert(*addr, channel);
        self.next_token();
    }

    fn receive_dispatch(&mut self) {
        while let Ok(event) = self.input_queue.try_recv() {
            self.handle_dispatch_event(event);
        }
    }

    fn handle_dispatch_event(&mut self, event: DispatchEvent) {
        match event {
            DispatchEvent::SendTcp(address, data) => {
                self.send_tcp_message(address, data);
            }
            DispatchEvent::SendUdp(address, data) => {
                self.send_udp_message(address, data);
            }
            DispatchEvent::Stop => {
                self.stop();
            }
            DispatchEvent::Kill => {
                self.kill();
            }
            DispatchEvent::Connect(addr) => {
                debug!(self.log, "Got DispatchEvent::Connect({})", addr);
                self.request_stream(addr);
            }
            DispatchEvent::ClosedAck(addr) => {
                debug!(self.log, "Got DispatchEvent::ClosedAck({})", addr);
                self.handle_closed_ack(addr);
            }
            DispatchEvent::Close(addr) => {
                self.close_connection(addr);
            }
        }
    }

    fn send_tcp_message(&mut self, address: SocketAddr, data: DispatchData) {
        if let Some(channel) = self.channel_map.get_mut(&address) {
            // The stream is already set-up, buffer the package and wait for writable event
            if channel.connected() {
                if let Ok(frame) = self.serialise_dispatch_data(data) {
                    channel.enqueue_serialised(frame);
                    channel.try_drain();
                } else {
                    // TODO: Out of Buffers for Lazy-serialisation?
                }
            } else {
                trace!(self.log, "Dispatch trying to route to non connected channel {:?}, rejecting the message", channel);
                self.reject_dispatch_data(address, data);
            }
        } else {
            trace!(self.log, "Dispatch trying to route to unrecognized address {}, rejecting the message", address);
            self.reject_dispatch_data(address, data);
        }
    }

    fn send_udp_message(&mut self, address: SocketAddr, data: DispatchData) {
        if let Some(ref mut udp_state) = self.udp_state {
            if let Ok(frame) = self.serialise_dispatch_data(data) {
                udp_state.enqueue_serialised(address, frame);
                match udp_state.try_write() {
                    Ok(n) => {
                        self.sent_bytes += n as u64;
                    }
                    Err(e) => {
                        warn!(self.log, "Error during UDP sending: {}", e);
                        debug!(self.log, "UDP erro debug info: {:?}", e);
                    }
                }
            } else {
                // TODO: Out of Buffers for Lazy-serialisation?
            }
        } else {
            self.reject_dispatch_data(address, data);
            trace!(self.log, "Rejecting UDP message to {} as socket is already shut down.", address);
        }
    }

    fn serialise_dispatch_data(&mut self, data: DispatchData) -> Result<SerialisedFrame, SerError> {
        match data {
            DispatchData::Serialised(frame) => {
                Ok(frame)
            }
            _ => {
                data.into_serialised(&mut self.encode_buffer.get_buffer_encoder()?)
            }
        }
    }

    /// Handles all logic necessary to shutdown a channel for which the connection has been lost.
    fn lost_connection(&mut self, addr: &SocketAddr) -> () {
        // We will only drop the Channel once we get the CloseAck from the NetworkDispatcher
        if let Some(channel) = self.channel_map.get_mut(&addr) {
            self.notify_connection_state(*addr, ConnectionState::Lost);
            for rejected_frame in channel.take_outbound() {
                self.reject_dispatch_data(*addr, DispatchData::Serialised(rejected_frame));
            }
            channel.shutdown();
        }
    }

    /// Initiates a graceful closing sequence
    fn close_connection(&mut self, addr: SocketAddr) -> () {
        if let Some(channel) = self.channel_map.get_mut(&addr) {
            // The channel may fail to perform its graceful shutdown if the Network
            // is unable to send the closing message now, in that case the channel will remain
            // open until it has been sent
            let _ = channel.initiate_graceful_shutdown();
        }
    }

    fn handle_closed_ack(&mut self, addr: SocketAddr) -> () {
        if let Some(mut channel) = self.channel_map.remove(&addr) {
            match channel.state {
                ChannelState::Connected(_, _) => {
                    error!(self.log, "ClosedAck for connected Channel: {:#?}", channel);
                    self.channel_map.insert(addr, channel);
                }
                _ => {
                    channel.shutdown();
                    let buffer = channel.destroy();
                    self.buffer_pool.return_buffer(buffer);
                }
            }
        }
    }

    fn stop(&mut self) -> () {
        let tokens = self.token_map.clone();
        for (_, addr) in tokens {
            // self.try_read(&addr);
        }
        for (_, mut channel) in self.channel_map.drain() {
            debug!(
                self.log,
                "Stopping channel with message count {}", channel.messages
            );
            let _ = channel.initiate_graceful_shutdown();
        }
        if let Some(mut listener) = self.tcp_listener.take() {
            self.poll.registry().deregister(&mut listener).ok();
            drop(listener);
            debug!(self.log, "Dropped its TCP server");
        }
        if let Some(mut udp_state) = self.udp_state.take() {
            self.poll.registry().deregister(&mut udp_state.socket).ok();
            let count = udp_state.pending_messages();
            drop(udp_state);
            debug!(
                self.log,
                "Dropped its UDP socket with message count {}", count
            );
        }
        self.stopped = true;
        debug!(self.log, "Stopped");
    }

    fn kill(&mut self) -> () {
        debug!(self.log, "Killing channels");
        for (_, channel) in self.channel_map.drain() {
            channel.kill();
        }
        self.stop();
    }

    fn next_token(&mut self) -> () {
        let next = self.token.0 + 1;
        self.token = Token(next);
    }
}

fn bind_with_retries(
    addr: &SocketAddr,
    retries: usize,
    log: &KompactLogger,
) -> io::Result<TcpListener> {
    match TcpListener::bind(*addr) {
        Ok(listener) => Ok(listener),
        Err(e) => {
            if retries > 0 {
                debug!(
                    log,
                    "Failed to bind to addr {}, will retry {} more times, error was: {:?}",
                    addr,
                    retries,
                    e
                );
                // Lets give cleanup some time to do it's thing before we retry
                thread::sleep(Duration::from_millis(BIND_RETRY_INTERVAL));
                bind_with_retries(addr, retries - 1, log)
            } else {
                Err(e)
            }
        }
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod tests {
    use super::*;
    use crate::{dispatch::NetworkConfig, net::buffers::BufferConfig};

    // Cleaner test-cases for manually running the thread
    fn poll_and_handle(thread: &mut NetworkThread) -> () {
        let mut events = Events::with_capacity(10);
        thread
            .poll
            .poll(&mut events, Some(Duration::from_millis(100)));
        for event in events.iter() {
            thread.handle_event(event.token(), event.is_readable(), event.is_writable(), 0);
        }
    }

    #[allow(unused_must_use)]
    fn setup_two_threads() -> (
        NetworkThread,
        Sender<DispatchEvent>,
        NetworkThread,
        Sender<DispatchEvent>,
    ) {
        let mut cfg = KompactConfig::new();
        cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());
        let system = cfg.build().expect("KompactSystem");

        // Set-up the the threads arguments
        let lookup = Arc::new(ArcSwap::from_pointee(ActorStore::new()));
        //network_thread_registration.set_readiness(Interest::empty());
        let (input_queue_1_sender, input_queue_1_receiver) = channel();
        let (input_queue_2_sender, input_queue_2_receiver) = channel();
        let (dispatch_shutdown_sender1, _) = promise();
        let (dispatch_shutdown_sender2, _) = promise();
        let logger = system.logger().clone();
        let dispatcher_ref = system.dispatcher_ref();

        // Set up the two network threads
        let (network_thread1, _) = NetworkThread::new(
            logger.clone(),
            "127.0.0.1:0".parse().expect("Address should work"),
            lookup.clone(),
            input_queue_1_receiver,
            dispatch_shutdown_sender1,
            dispatcher_ref.clone(),
            NetworkConfig::default(),
        );

        let (network_thread2, _) = NetworkThread::new(
            logger,
            "127.0.0.1:0".parse().expect("Address should work"),
            lookup,
            input_queue_2_receiver,
            dispatch_shutdown_sender2,
            dispatcher_ref,
            NetworkConfig::default(),
        );
        (
            network_thread1,
            input_queue_1_sender,
            network_thread2,
            input_queue_2_sender,
        )
    }

    #[test]
    fn merge_connections_basic() -> () {
        // Sets up two NetworkThreads and does mutual connection request
        let (mut thread1, input_queue_1_sender, mut thread2, input_queue_2_sender) =
            setup_two_threads();
        let addr1 = thread1.addr;
        let addr2 = thread2.addr;
        // Tell both to connect to each-other before they start running:
        input_queue_1_sender.send(DispatchEvent::Connect(addr2));
        input_queue_2_sender.send(DispatchEvent::Connect(addr1));

        // Let both handle the connect event:
        thread1.receive_dispatch();
        thread2.receive_dispatch();

        // Wait for the connect requests to reach destination:
        thread::sleep(Duration::from_millis(100));

        // Accept requested streams
        thread1.receive_stream();
        thread2.receive_stream();

        // Wait for Hello to reach destination:
        thread::sleep(Duration::from_millis(100));

        // We need to make sure the TCP buffers are actually flushing the messages.
        // Handle events on both ends, say hello:
        poll_and_handle(&mut thread1);
        poll_and_handle(&mut thread2);
        thread::sleep(Duration::from_millis(100));
        // Cycle two Requested channels
        poll_and_handle(&mut thread1);
        poll_and_handle(&mut thread2);
        thread::sleep(Duration::from_millis(100));
        // Cycle three, merge and close
        poll_and_handle(&mut thread1);
        poll_and_handle(&mut thread2);
        thread::sleep(Duration::from_millis(100));
        // Cycle four, receive close and close
        poll_and_handle(&mut thread1);
        poll_and_handle(&mut thread2);
        thread::sleep(Duration::from_millis(100));
        // Now we can inspect the Network channels, both only have one channel:
        assert_eq!(thread1.channel_map.len(), 1);
        assert_eq!(thread2.channel_map.len(), 1);

        // Now assert that they've kept the same channel:
        assert_eq!(
            thread1
                .channel_map
                .drain()
                .next()
                .unwrap()
                .1
                .stream()
                .local_addr()
                .unwrap(),
            thread2
                .channel_map
                .drain()
                .next()
                .unwrap()
                .1
                .stream()
                .peer_addr()
                .unwrap()
        );
    }

    #[test]
    fn merge_connections_tricky() -> () {
        // Sets up two NetworkThreads and does mutual connection request
        // This test uses a different order of events than basic
        let (mut thread1, input_queue_1_sender, mut thread2, input_queue_2_sender) =
            setup_two_threads();
        let addr1 = thread1.addr;
        let addr2 = thread2.addr;
        // 2 Requests connection to 1 and sends Hello
        input_queue_2_sender.send(DispatchEvent::Connect(addr1));
        thread2.receive_dispatch();
        thread::sleep(Duration::from_millis(100));

        // 1 accepts the connection and sends hello back
        thread1.receive_stream();
        thread::sleep(Duration::from_millis(100));
        // 2 receives the Hello
        poll_and_handle(&mut thread2);
        thread::sleep(Duration::from_millis(100));
        // 1 Receives Hello
        poll_and_handle(&mut thread1);

        // 1 Receives Request Connection Event, this is the tricky part
        // 1 Requests connection to 2 and sends Hello
        input_queue_1_sender.send(DispatchEvent::Connect(addr2));
        thread1.receive_dispatch();
        thread::sleep(Duration::from_millis(100));

        // 2 accepts the connection and replies with hello
        thread2.receive_stream();
        thread::sleep(Duration::from_millis(100));

        // 2 receives the Hello on the new channel and merges
        poll_and_handle(&mut thread2);
        thread::sleep(Duration::from_millis(100));

        // 1 receives the Hello on the new channel and merges
        poll_and_handle(&mut thread1);
        thread::sleep(Duration::from_millis(100));

        // 2 receives the Bye and the Ack.
        poll_and_handle(&mut thread2);
        thread::sleep(Duration::from_millis(100));

        // Now we can inspect the Network channels, both only have one channel:
        assert_eq!(thread1.channel_map.len(), 1);
        assert_eq!(thread2.channel_map.len(), 1);

        // Now assert that they've kept the same channel:
        assert_eq!(
            thread1
                .channel_map
                .drain()
                .next()
                .unwrap()
                .1
                .stream()
                .local_addr()
                .unwrap(),
            thread2
                .channel_map
                .drain()
                .next()
                .unwrap()
                .1
                .stream()
                .peer_addr()
                .unwrap()
        );
    }

    #[test]
    fn network_thread_custom_buffer_config() -> () {
        let addr = "127.0.0.1:0".parse().expect("Address should work");
        let mut buffer_config = BufferConfig::default();
        buffer_config.chunk_size(128);
        buffer_config.max_chunk_count(14);
        buffer_config.initial_chunk_count(13);
        buffer_config.encode_buf_min_free_space(10);
        let network_config = NetworkConfig::with_buffer_config(addr, buffer_config);
        let mut cfg = KompactConfig::new();
        cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());
        let system = cfg.build().expect("KompactSystem");

        // Set-up the the threads arguments
        // TODO: Mock this properly instead
        let lookup = Arc::new(ArcSwap::from_pointee(ActorStore::new()));
        //network_thread_registration.set_readiness(Interest::empty());
        let (_, input_queue_1_receiver) = channel();
        let (dispatch_shutdown_sender1, _) = promise();
        let logger = system.logger().clone();
        let dispatcher_ref = system.dispatcher_ref();

        // Set up the two network threads
        let (mut network_thread, _) = NetworkThread::new(
            logger,
            addr,
            lookup,
            input_queue_1_receiver,
            dispatch_shutdown_sender1,
            dispatcher_ref,
            network_config,
        );
        // Assert that the buffer_pool is created correctly
        let (pool_size, _) = network_thread.buffer_pool.get_pool_sizes();
        assert_eq!(pool_size, 13); // initial_pool_size
        assert_eq!(network_thread.buffer_pool.get_buffer().unwrap().len(), 128);
        network_thread.stop();
    }

    /*
    #[test]
    fn graceful_network_shutdown() -> () {
        // Sets up two NetworkThreads and connects them to eachother, then shuts it down

        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8878);
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8880);

        let (mut network_thread1, input_queue_1_sender, mut network_thread2, input_queue_2_sender) = setup_two_threads(addr1.clone(), addr2.clone());

        // 2 Requests connection to 1 and sends Hello
        input_queue_2_sender.send(DispatchEvent::Connect(addr1.clone()));
        network_thread2.receive_dispatch();
        thread::sleep(Duration::from_millis(100));
        network_thread1.accept_stream();
        // Hello is sent to network_thread2, let it read:
        network_thread2.poll.poll(&mut events, None);
        for event in events.iter() {
            network_thread2.handle_event(event);
        }
        events.clear();
    }*/
}
