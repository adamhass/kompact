use super::*;
use std::sync::mpsc::{channel, Receiver as Recv, Sender};
use std::{collections::{HashMap, VecDeque, LinkedList}, net::SocketAddr, usize, vec::Vec, io::{self, Read, Write, Error, ErrorKind}, mem};
use mio::{{Events, Poll, Ready, PollOpt, Token, Registration}, event::Event, net::{TcpListener, TcpStream}, Evented};
use crate::net::{
    frames::*,
    buffer_pool::BufferPool,
    buffer::DecodeBuffer,
    ConnectionState,
};
use crossbeam_queue::SegQueue;
use iovec::IoVec;
use std::borrow::Borrow;
use bytes::{BytesMut, BufMut, Buf};
use std::ops::Deref;
use std::borrow::BorrowMut;
use std::net::Shutdown::Both;
use std::time::Duration;
use fxhash::{FxHashMap, FxHasher};
use std::hash::BuildHasherDefault;
use crate::net::network_channel::TcpChannel;
use crate::messaging::{DispatchEnvelope, EventEnvelope};


/*
    Using https://github.com/tokio-rs/mio/blob/master/examples/tcp_server.rs as template.
    Single threaded MIO event loop.
    Receives outgoing Box<IoVec> via SegQueue, notified by polling via the "Registration"
    Buffers the outgoing Boxes for different connections in a hashmap.
    Will Send incoming messages directly to components.
*/

// Used for identifying connections
const SERVER: Token = Token(0);
const START_TOKEN: Token = Token(1);
// Used for identifying the dispatcher/input queue
const DISPATCHER: Token = Token(usize::MAX-1);
const READ_BUFFER_SIZE: usize = 655355;
const MAX_POLL_EVENTS: usize = 1024;
pub const MAX_INTERRUPTS: i32 = 9;

pub struct NetworkThread {
    log: KompactLogger,
    pub addr: SocketAddr,
    //connection_events: UnboundedSender<NetworkEvent>,
    lookup: Arc<ArcSwap<ActorStore>>,
    listener: Box<TcpListener>,
    poll: Poll,
    // Contains K,V=Remote SocketAddr, Output buffer; Token for polling; Input-buffer,
    channel_map: FxHashMap<SocketAddr, TcpChannel>,
    token_map: FxHashMap<Token, SocketAddr>,
    token: Token,
    input_queue: Box<Recv<DispatchEvent>>,
    dispatcher_registration: Registration,
    dispatcher_set_readiness: SetReadiness,
    dispatcher_ref: DispatcherRef,
    buffer_pool: BufferPool,
    sent_bytes: u64,
    received_bytes: u64,
    sent_msgs: u64,
    received_msgs: u64,
    stopped: bool,
    network_thread_sender: Sender<bool>,
}

impl NetworkThread {
    pub fn new(
        log: KompactLogger,
        addr: SocketAddr,
        //connection_events: UnboundedSender<NetworkEvent>,
        lookup: Arc<ArcSwap<ActorStore>>,
        dispatcher_registration: Registration,
        dispatcher_set_readiness: SetReadiness,
        input_queue: Recv<DispatchEvent>,
        network_thread_sender: Sender<bool>,
        dispatcher_ref: DispatcherRef,
    ) -> NetworkThread {
        // Set-up the Listener
        if let Ok(listener) = TcpListener::bind(&addr) {
            let actual_addr = listener
                .local_addr()
                .expect("could not get real addr");
            // Set up polling for the Dispatcher and the listener.
            let mut poll = Poll::new()
                .expect("failed to create Poll instance in NetworkThread");
            poll.register(&listener, SERVER, Ready::readable(), PollOpt::edge() | PollOpt::oneshot())
                .expect("failed to register TCP SERVER");
            poll.register(&dispatcher_registration, DISPATCHER, Ready::readable(), PollOpt::edge() | PollOpt::oneshot())
                .expect("failed to register dispatcher Poll");
            // Connections and buffers
            // There's probably a better way to handle the token/addr maps
            pub type FxBuildHasher = BuildHasherDefault<FxHasher>;
            let mut channel_map: FxHashMap<SocketAddr, TcpChannel> = HashMap::<SocketAddr, TcpChannel, FxBuildHasher>::default();
            let mut token_map: FxHashMap<Token, SocketAddr> = HashMap::<Token, SocketAddr, FxBuildHasher>::default();
            NetworkThread {
                log,
                addr: actual_addr,
                //connection_events,
                lookup,
                listener: Box::new(listener),
                poll,
                channel_map,
                token_map,
                token: START_TOKEN,
                input_queue: Box::new(input_queue),
                dispatcher_registration,
                dispatcher_set_readiness,
                buffer_pool: BufferPool::new(),
                sent_bytes: 0,
                received_bytes: 0,
                sent_msgs: 0,
                received_msgs: 0,
                stopped: false,
                network_thread_sender,
                dispatcher_ref,
            }
        } else {
            panic!("NetworkThread failed to bind to address!");
        }

    }

    // Event loop
    pub fn run(&mut self) -> () {
        let mut events = Events::with_capacity(MAX_POLL_EVENTS);
        debug!(self.log, "NetworkThread entering main EventLoop");
        loop {
            self.poll.poll(&mut events, None)
                .expect("Error when calling Poll");

            for event in events.iter() {
                //println!("{} Handling event", self.addr);
                if let Some(hello_addr) = self.handle_event(event) {
                    // Handle channel greeting, make sure to merge channels and only retain one per address
                    //println!("NetworkThread {} got hello from {}", self.addr, &hello_addr);
                    // Listening on the event returned a Hello msg with addr
                    if let Some(registered_addr) = self.token_map.remove(&event.token()) {
                        debug!(self.log, "NetworkThread {} got Hello({}) from {}", self.addr, &hello_addr, &registered_addr);
                        if hello_addr == registered_addr {
                            // The channel is already correct, we update state at the end
                        } else {
                            if let Some(mut channel) = self.channel_map.remove(&hello_addr) {
                                if let Some(mut other_channel) = self.channel_map.remove(&registered_addr) {
                                    // We already have a channel for the "Hello_addr", this is not great.
                                    // Merge the connections into one
                                    debug!(self.log, "NetworkThread {} merging {} into {}", self.addr, &registered_addr, &hello_addr);
                                    channel.merge( &mut other_channel);
                                    self.channel_map.insert(hello_addr, channel );

                                } else {
                                    self.channel_map.insert(hello_addr, channel);
                                }

                            } else if let Some(channel) = self.channel_map.remove(&registered_addr) {
                                // Re-insert the channel with different key
                                self.channel_map.insert(hello_addr, channel);
                            }
                        }

                        // Make sure that the channel is registered correctly and in Connected State.
                        if let Some(channel) = self.channel_map.get_mut(&hello_addr) {
                            channel.state = ConnectionState::Connected(hello_addr);
                            channel.token = event.token();
                            self.token_map.insert(event.token(), hello_addr);
                            //println!("Telling dispatcher about the Connected peer!");
                            self.dispatcher_ref.tell(
                                DispatchEnvelope::Event(
                                    EventEnvelope::Network(
                                        NetworkEvent::Connection(hello_addr, ConnectionState::Connected(hello_addr)))));
                        }
                    } else {
                        panic!("No address registered for a token which yielded a hello msg");
                    }
                    self.try_read(&hello_addr);
                }
                if self.stopped {
                    self.network_thread_sender.send(true);
                    debug!(self.log, "NetworkThread {} Stopped", self.addr);
                    return
                };
            }
        }
    }

    fn handle_event(&mut self, event: Event) -> Option<SocketAddr> {
        match event.token() {
            SERVER => {
                // Received an event for the TCP server socket.Accept the connection.
                if let Err(e) = self.accept_stream() {
                    //println!("error accepting stream:\n {:?}", e);
                }
                // Listen for more
                self.poll.register(&self.listener, SERVER, Ready::readable(), PollOpt::edge() | PollOpt::oneshot());
            }
            DISPATCHER => {
                // Message available from Dispatcher, clear the poll readiness before receiving
                self.dispatcher_set_readiness.set_readiness(Ready::empty());
                if let Err(e) = self.receive_dispatch() {
                    println!("error receiving from dispatch");
                };
                // Reregister polling
                self.poll.register(&self.dispatcher_registration, DISPATCHER, Ready::readable(), PollOpt::edge() | PollOpt::oneshot());
            }
            token => {
                // lookup token state in kv map <token, state> (it's corresponding addr for now)
                let addr = {
                    if let Some(addr) = self.token_map.get(&token) {addr.clone() }
                    else {panic!("does not recognize connection token");}
                };
                if event.readiness().is_writable() {
                    self.try_write(&addr);
                }
                if event.readiness().is_readable() {
                    return self.try_read(&addr);
                }
            }
        }
        None
    }

    fn try_write(&mut self, addr: &SocketAddr) -> () {
        if let Some(channel) = self.channel_map.get_mut(&addr) {
            match channel.try_drain() {
                Err(ref err) if broken_pipe(err) => {
                    println!("BROKEN PIPE WRITE");
                }
                Ok(n) => {
                    self.sent_bytes = self.sent_bytes + n as u64;
                    if channel.has_remaining_output() {
                        self.poll.register(channel.stream(), channel.token.clone(), Ready::writable(), PollOpt::edge() | PollOpt::oneshot());
                    }
                }
                Err(e) => {
                    println!("Error while sending! {:?}", e);
                }
            }
        }
    }

    fn try_read(&mut self, addr: &SocketAddr) -> Option<SocketAddr> {
        //println!("{} channel.receive()", self.addr);
        if let Some(channel) = self.channel_map.get_mut(&addr) {
            match channel.receive() {
                Ok(0) => {
                    // Eof?
                }
                Ok(n) => {
                    //println!("read {} bytes", n);
                    self.received_bytes += n as u64;
                    self.poll.register(channel.stream(), channel.token.clone(), Ready::readable(), PollOpt::edge() | PollOpt::oneshot());
                }
                Err(ref err) if no_buffer_space(err) => {
                    debug!(self.log, "NetworkThread {} no_buffer_space for channel to {}", self.addr, &addr);
                    // Buffer full, we swap it and register for poll again
                    if let Some(mut new_buffer) = self.buffer_pool.get_buffer() {
                        channel.swap_buffer(&mut new_buffer);
                        self.buffer_pool.return_buffer(new_buffer);
                    }
                    // The poll will activate again if the bytes are still there
                    self.poll.register(channel.stream(), channel.token.clone(), Ready::readable(), PollOpt::edge() | PollOpt::oneshot());
                }
                Err(err) if interrupted(&err) || would_block(&err) => {
                    // Just retry later
                    self.poll.register(channel.stream(), channel.token.clone(), Ready::readable(), PollOpt::edge() | PollOpt::oneshot());
                }
                Err(err) if connection_reset(&err) => {
                    debug!(self.log, "NetworkThread {} Connection_reset to peer {}, shutting down the channel", self.addr, &addr);
                    channel.shutdown();
                    self.dispatcher_ref.tell(
                        DispatchEnvelope::Event(
                            EventEnvelope::Network(
                                NetworkEvent::Connection(*addr, ConnectionState::Closed))));
                }
                Err(err) => {
                    // Fatal error don't try to read again
                    debug!(self.log, "NetworkThread {} Error while reading from peer {}:\n{:?}", self.addr, &addr, &err);
                }
            }
            let mut msgs = 0;
            //println!("{} channel.decode()", self.addr);
            while let Some(frame) = channel.decode() {
                self.received_msgs = msgs;
                match frame {
                    Frame::Data(fr) => {
                        // Forward the data frame to the correct actor
                        //println!("{} data frame...", self.addr);
                        let lease_lookup = self.lookup.lease();
                        {
                            use dispatch::lookup::ActorLookup;
                            use serialisation::helpers::deserialise_msg;
                            let buf = fr.payload();
                            let mut envelope = deserialise_msg(buf).expect("s11n errors");
                            match lease_lookup.get_by_actor_path(envelope.receiver()) {
                                None => {
                                    println!(
                                        "Could not find actor reference for destination: {:?}",
                                        envelope.receiver());
                                }
                                Some(actor) => {
                                    msgs = msgs + 1;
                                    actor.enqueue(envelope);
                                }
                            }
                        }
                    },
                    Frame::Hello(hello) => {
                        //println!("Got hello message!");
                        return Some(hello.addr());
                    },
                    Frame::Bye() => {
                        debug!(self.log, "NetworkThread {} received Bye from {}", self.addr, &addr);
                        channel.shutdown();
                        self.dispatcher_ref.tell(
                            DispatchEnvelope::Event(
                                EventEnvelope::Network(
                                    NetworkEvent::Connection(*addr, ConnectionState::Closed))));
                    }
                    _ => {
                        debug!(self.log, "NetworkThread {} unexpected frame type from {}", self.addr, &addr);
                    },
                }
            }
        }
        None
    }

    fn request_stream(&mut self, addr: SocketAddr) -> io::Result<bool> {
        debug!(self.log, "NetworkThread {} requesting connection to {}", self.addr, &addr);
        let stream = TcpStream::connect(&addr)?;
        self.store_stream(stream, &addr);
        Ok(true)
    }

    fn accept_stream(&mut self) -> io::Result<bool> {
        while let (stream, addr) = self.listener.accept()? {
            debug!(self.log, "NetworkThread {} accepting connection from {}", self.addr, &addr);
            self.store_stream(stream, &addr);
        }
        Ok(true)
    }

    fn store_stream(&mut self, stream: TcpStream, addr: &SocketAddr) -> () {
        if let Some(buffer) = self.buffer_pool.get_buffer() {
            self.token_map.insert(self.token.clone(), addr.clone());
            let mut channel = TcpChannel::new(stream, self.token, buffer);
            debug!(self.log, "NetworkThread {} saying Hello to {}", self.addr, &addr);
            channel.initialize(&self.addr);
            self.poll.register(channel.stream(), self.token.clone(), Ready::readable() | Ready::writable(), PollOpt::edge() | PollOpt::oneshot());
            self.channel_map.insert(
                addr.clone(),
                channel,
            );
            self.next_token();
        } else {
            // TODO: Handle gracefully
            panic!("Unable to store a stream, no buffers available!");
        }
    }

    fn receive_dispatch(&mut self) -> io::Result<bool> {
        while let Ok(event) = self.input_queue.try_recv() {
            //println!("{} Received DispatchEvent", self.addr);
            match event {
                DispatchEvent::Send(addr, packet) => {
                    self.sent_msgs += 1;
                    // Get the token corresponding to the connection
                    if let Some(channel) = self.channel_map.get_mut(&addr) {
                        // The stream is already set-up, buffer the package and wait for writable event
                        channel.enqueue_serialized(packet);
                        channel.try_drain();
                        if channel.has_remaining_output() {
                            self.poll.register(channel.stream(), channel.token.clone(), Ready::writable(), PollOpt::edge() | PollOpt::oneshot());
                        }
                    } else {
                        // The stream isn't set-up, request connection, set-it up and try to send the message
                        self.request_stream(addr.clone());
                        if let Some(channel) = self.channel_map.get_mut(&addr) {
                            channel.enqueue_serialized(packet);
                            channel.try_drain();
                            if channel.has_remaining_output() {
                                self.poll.register(channel.stream(), channel.token.clone(), Ready::writable(), PollOpt::edge() | PollOpt::oneshot());
                            }
                        }
                    }
                },
                DispatchEvent::Stop() => {
                    self.stop();
                },
                DispatchEvent::Connect(addr) => {
                    self.request_stream(addr.clone());
                },
            }
        }
        Ok(true)
    }

    fn stop(&mut self) -> () {
        debug!(self.log, "NetworkThread {} Stopping...", self.addr);
        let tokens = self.token_map.clone();
        for (_, addr) in tokens {
            self.try_read(&addr);
        }
        for (_, mut channel) in self.channel_map.drain() {
            match channel.state {
                ConnectionState::Closed => {

                },
                _ => {
                    channel.graceful_shutdown();
                }
            }
        }
        self.stopped = true;
    }

    fn next_token(&mut self) -> () {
        let next = self.token.0 + 1;
        self.token = Token(next);
    }
}

// Error handling helper functions
pub fn would_block(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::WouldBlock
}

pub fn interrupted(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::Interrupted
}

pub fn no_buffer_space(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::InvalidData
}

pub fn connection_reset(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::ConnectionReset
}

pub fn broken_pipe(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::BrokenPipe
}

#[cfg(test)]
mod tests {
    use crate::prelude::*;

    use mio::{Registration, Ready};
    use std::sync::mpsc::{channel, Sender, Receiver as Recv};
    use crate::net::network_thread::NetworkThread;
    use std::thread;
    use std::net::{SocketAddr, IpAddr, Ipv4Addr};
    use std::sync::Arc;
    use std::time::Duration;
    use arc_swap::ArcSwap;
    use crate::dispatch::lookup::ActorStore;
    use crate::KompactLogger;

    /* #[test]
    fn network_thread_no_local_poll1() -> () {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7778);
        let (registration, network_thread_registration) = Registration::new2();
        network_thread_registration.set_readiness(Ready::empty());
        let (sender, receiver) = channel();
        let actor_store = ActorStore::new();
        let lookup = Arc::new(ArcSwap::from(Arc::new(ActorStore::new())));
        let mut network_thread = NetworkThread::new(
            //KompactLogger::new(),
            addr,
            lookup.clone(),
            registration,
            network_thread_registration.clone(),
            receiver,
        );
        thread::spawn(move || {
            network_thread.run();
        });
        //println!("network_thread spawned");
        thread::sleep(Duration::from_secs(1));
        //println!("setting readiness");
        network_thread_registration.set_readiness(Ready::readable());
        thread::sleep(Duration::from_secs(1));
        //println!("clearing readiness");
        network_thread_registration.set_readiness(Ready::empty());
        thread::sleep(Duration::from_secs(1));
        //println!("setting readiness again");
        network_thread_registration.set_readiness(Ready::readable());
        thread::sleep(Duration::from_secs(1));
    } */
}