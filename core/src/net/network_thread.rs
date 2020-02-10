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
use crate::actors::Transport::TCP;
use crate::serialisation::serialisation_ids::SYSTEM_PATH;
use mio::tcp::Shutdown;
use crate::messaging::{DispatchEnvelope, EventEnvelope};
use crate::net::events::NetworkEvent;
//use crate::net::events::NetworkEvent;

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
const MAX_INTERRUPTS: i32 = 9;

pub struct NetworkThread {
    //log: KompactLogger,
    pub addr: SocketAddr,
    //connection_events: UnboundedSender<NetworkEvent>,
    lookup: Arc<ArcSwap<ActorStore>>,
    listener: Box<TcpListener>,
    poll: Poll,
    // Contains K,V=Remote SocketAddr, Output buffer; Token for polling; Input-buffer,
    stream_map: FxHashMap<SocketAddr, (TcpStream, VecDeque<SerializedFrame>, Token, DecodeBuffer)>,
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
        //log: KompactLogger,
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
            let mut stream_map: FxHashMap<SocketAddr, (TcpStream, VecDeque<SerializedFrame>, Token, DecodeBuffer)> =
                HashMap::<SocketAddr, (TcpStream, VecDeque<SerializedFrame>, Token, DecodeBuffer), FxBuildHasher>::default();
            let mut token_map: FxHashMap<Token, SocketAddr> = HashMap::<Token, SocketAddr, FxBuildHasher>::default();
            NetworkThread {
                //log,
                addr: actual_addr,
                //connection_events,
                lookup,
                listener: Box::new(listener),
                poll,
                stream_map,
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
        println!("Network thread starting loop");
        loop {
            self.poll.poll(&mut events, None)
                .expect("Error when calling Poll");

            for event in events.iter() {
                println!("Handling event");
                if let Some(hello_addr) = self.handle_event(event) {
                    println!("NetworkThread {} got hello from {}", self.addr, &hello_addr);
                    // Listening on the event returned a Hello msg with addr
                    if let Some(registered_addr) = self.token_map.remove(&event.token()) {
                        if hello_addr == registered_addr {
                            // Expected case, we can carry on...
                            self.token_map.insert(event.token(), hello_addr);
                        } else {
                            // Merge the connections into one
                            println!("Merging two connections");
                            if let Some((mut old_stream,
                                            mut old_output_buffer,
                                            old_token,
                                            mut old_decode_buffer)) = self.stream_map.remove(&registered_addr) {
                                if let Some((mut stream,
                                                mut output_buffer,
                                                token,
                                                mut decode_buffer)) = self.stream_map.remove(&hello_addr) {
                                    // Got full info of both streams
                                    if (old_output_buffer.len() > 0 && output_buffer.len() > 0) || (old_decode_buffer.len() > 0 && decode_buffer.len() > 0) {
                                        // We've broken fifo ordering, this is unacceptable
                                        panic!("FIFO NOT GUARANTEED WHEN MERGING STREAMS!!");
                                    }
                                    if old_output_buffer.len() > 0 {
                                        output_buffer.append(&mut old_output_buffer);
                                        println!("Appended old_output_buffer {} to new {}", old_output_buffer.len(), output_buffer.len());
                                    }
                                    self.poll.register(&stream, token.clone(), Ready::readable() | Ready::writable(), PollOpt::edge() | PollOpt::oneshot());
                                    if old_decode_buffer.len() > 0 {
                                        println!("Keep old_decodebuffer len: {} content: {:?}", old_decode_buffer.len(), old_decode_buffer.get_buffer_head());
                                        // Re-insert the connection with old decode
                                        self.stream_map.insert(hello_addr, (stream, output_buffer, token, old_decode_buffer));
                                    } else {
                                        // Re-insert the connection with the new decode
                                        println!("Use new decodebuffer len: {} content: {:?}", decode_buffer.len(), decode_buffer.get_buffer_head());
                                        self.stream_map.insert(hello_addr, (stream, output_buffer, token, decode_buffer));
                                    }
                                    self.poll.deregister(&old_stream);
                                    old_stream.shutdown(Shutdown::Both);
                                } else {
                                    // We only have the old stream set-up, re-insert it with the proper handle:
                                    self.poll.register(&old_stream, old_token.clone(), Ready::readable() | Ready::writable(), PollOpt::edge() | PollOpt::oneshot());
                                    self.stream_map.insert(hello_addr.clone(), (old_stream, old_output_buffer, old_token.clone(), old_decode_buffer));
                                    self.token_map.remove(&old_token);
                                    self.token_map.insert(old_token, hello_addr);
                                }
                            }
                        }
                        self.dispatcher_ref.tell(
                            DispatchEnvelope::Event(
                                EventEnvelope::Network(
                                    NetworkEvent::Connection(hello_addr, ConnectionState::Connected(hello_addr)))));
                    } else {
                        panic!("No address registered for a token which yielded a hello msg");
                    }
                }
                if self.stopped {
                    self.network_thread_sender.send(true);
                    return
                };
            }
        }
    }

    fn handle_event(&mut self, event: Event) -> Option<SocketAddr> {
        let mut retry = false;
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
                    if let Some(addr) = self.token_map.get(&token) {addr }
                    else {panic!("does not recognize connection token");}
                };
                println!("Network Thread with self.addr {} handling token for remote address {}", self.addr, addr);
                if let Some((stream, out_buffer, _, in_buffer)) = self.stream_map.get_mut(&addr) {
                    if event.readiness().is_writable() {
                        //println!("sending buffer");
                        match Self::send_buffer(stream, out_buffer) {
                            Err(ref err) if broken_pipe(err) => {
                                println!("BROKEN PIPE WRITE");
                            }
                            Ok(n) => {
                                self.sent_bytes = self.sent_bytes + n as u64;
                                if !out_buffer.is_empty() {
                                    self.poll.register(stream, token.clone(), Ready::writable(), PollOpt::edge() | PollOpt::oneshot());
                                }
                            }
                            Err(e) => {
                                println!("Error while sending! {:?}", e);
                            }
                        }

                    }
                    if event.readiness().is_readable() {
                        match Self::receive(stream, in_buffer) {
                            Ok(0) => {
                                // Eof?
                            }
                            Ok(n) => {
                                //println!("read {} bytes", n);
                                self.received_bytes += n as u64;
                                self.poll.register(stream, token.clone(), Ready::readable(), PollOpt::edge() | PollOpt::oneshot());
                                if let Some(addr) = Self::decode(in_buffer, &mut self.lookup) {
                                    return Some(addr)
                                }
                            }
                            Err(ref err) if no_buffer_space(err) => {
                                // Buffer full, we swap it and register for poll again
                                if let Some(addr) = Self::decode(in_buffer, &mut self.lookup) {
                                    return Some(addr)
                                }
                                if let Some(mut new_buffer) = self.buffer_pool.get_buffer() {
                                    in_buffer.swap_buffer(&mut new_buffer);
                                    self.buffer_pool.return_buffer(new_buffer);
                                }
                                self.poll.register(stream, token.clone(), Ready::readable(), PollOpt::edge() | PollOpt::oneshot());
                            }
                            Err(err) if interrupted(&err) || would_block(&err) => {
                                // Just retry later
                                self.poll.register(stream, token.clone(), Ready::readable(), PollOpt::edge() | PollOpt::oneshot());
                            }
                            Err(err) => {
                                // Fatal error don't try to read again
                                println!("network thread {} Error while reading, write_offset {} received msgs: {}\n {:?}", self.addr, in_buffer.get_write_offset(), self.received_msgs, err);
                            }
                        }
                    }
                }
            }
        }
        None
    }

    fn request_stream(&mut self, addr: SocketAddr) -> io::Result<bool> {
        let stream = TcpStream::connect(&addr)?;
        self.store_stream(stream, &addr);
        Ok(true)
    }

    fn accept_stream(&mut self) -> io::Result<bool> {
        while let (stream, addr) = self.listener.accept()? {
            self.store_stream(stream, &addr);
        }
        Ok(true)
    }

    fn store_stream(&mut self, stream: TcpStream, addr: &SocketAddr) -> () {
        if let Some(buffer) = self.buffer_pool.get_buffer() {
            let mut in_buffer = DecodeBuffer::new(buffer);
            self.poll.register(&stream, self.token.clone(), Ready::readable() | Ready::writable(), PollOpt::edge() | PollOpt::oneshot());
            stream.set_nodelay(true);
            let mut send_queue = VecDeque::<SerializedFrame>::new();
            // Enqueue hello message:
            println!("Encoding hello");
            let hello = Frame::Hello(Hello::new(self.addr));
            let mut hello_bytes = BytesMut::with_capacity(hello.encoded_len());
            //hello_bytes.extend_from_slice(&[0;hello.encoded_len()]);
            if let Ok(()) = hello.encode_into(&mut hello_bytes) {
                println!("Encoded hello: {:?}", hello_bytes);
                send_queue.push_back(SerializedFrame::Bytes(hello_bytes.freeze()));
            } else {
                panic!("Unable to send hello bytes, failed to encode!");
            }
            self.token_map.insert(self.token.clone(), addr.clone());
            self.stream_map.insert(
                addr.clone(),
                (
                    stream,
                    send_queue,
                    self.token.clone(),
                    in_buffer,
                )
            );
            self.next_token();
        } else {
            // TODO: Handle gracefully
            panic!("Unable to store a stream, no buffers available!");
        }
    }

    fn receive_dispatch(&mut self) -> io::Result<bool> {
        while let Ok(event) = self.input_queue.try_recv() {
            match event {
                DispatchEvent::Send(addr, packet) => {
                    self.sent_msgs += 1;
                    // Get the token corresponding to the connection
                    if let Some((stream, buffer, token, _)) = self.stream_map.get_mut(&addr) {
                        // The stream is already set-up, buffer the package and wait for writable event
                        buffer.push_back(packet);
                        Self::send_buffer(stream, buffer);
                        self.poll.register(stream, token.clone(), Ready::writable(), PollOpt::edge() | PollOpt::oneshot());
                    } else {
                        // The stream isn't set-up, request connection, set-it up and try to send the message

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

    /// Returns `true` if the connection is done.
    fn receive(
        stream: &mut TcpStream,
        in_buffer: &mut DecodeBuffer,
    ) -> io::Result<usize> {
        let mut read_bytes = 0;
        let mut sum_read_bytes = 0;
        let mut interrupts = 0;
        println!("Network thread receiving on local stream {} from remote {}", stream.local_addr().unwrap(), stream.peer_addr().unwrap());
        loop {
            if let Some(mut io_vec) = in_buffer.get_writeable() {
                match stream.read_bufs(&mut [&mut io_vec]) {
                    Ok(0) => {
                        return Ok(sum_read_bytes)
                    },
                    Ok(n) => {
                        sum_read_bytes = sum_read_bytes + n;
                        read_bytes = n;
                        // continue looping and reading
                    },
                    Err(err) if would_block(&err) => {
                        return Ok(sum_read_bytes)
                    },
                    Err(err) if interrupted(&err) => {
                        // We should continue trying until no interruption
                        interrupts += 1;
                        if interrupts >= MAX_INTERRUPTS {
                            return Err(err)
                        }
                    },
                    Err(err) => {
                        println!("network thread {} got uncaught error during read from remote {}", stream.local_addr().unwrap(), stream.peer_addr().unwrap());
                        return Err(err)
                    }
                }
            } else {
                return Err(Error::new(ErrorKind::InvalidData, "No space in Buffer"));
            }
            // Keep all the read bytes in the buffer without overwriting
            if read_bytes > 0 {
                in_buffer.advance_writeable(read_bytes);
            }
        }
    }

    fn decode(in_buffer: &mut DecodeBuffer, lookup: &mut Arc<ArcSwap<ActorStore>>) -> Option<SocketAddr> {
        while let Some(mut frame) = in_buffer.get_frame() {
            match frame {
                Frame::Data(fr) => {
                    // Forward the data frame to the correct actor
                    let lease_lookup = lookup.lease();
                    {
                        use dispatch::lookup::ActorLookup;
                        use serialisation::helpers::deserialise_msg;
                        let buf = fr.payload();
                        let mut envelope = deserialise_msg(buf).expect("s11n errors");
                        println!("Network Thread handling envelope with sender {} and receiver {}", envelope.sender, envelope.receiver);
                        match lease_lookup.get_by_actor_path(envelope.receiver()) {
                            None => {
                                println!(
                                    "Could not find actor reference for destination: {:?}",
                                    envelope.receiver());
                            }
                            Some(actor) => {
                                actor.enqueue(envelope);
                            }
                        }
                    }
                },
                Frame::Hello(hello) => {
                    println!("Got hello message!");
                    return Some(hello.addr());
                }
                _ => {
                    println!("Unexpected frame type");
                },
            }
        };
        None
    }

    fn stop(&mut self) -> () {
        for (_, (mut stream, mut out_buffer, _, mut in_buffer)) in self.stream_map.drain() {
            Self::receive(&mut stream, &mut in_buffer);
            Self::decode(&mut in_buffer, &mut self.lookup);
            Self::send_buffer(&mut stream, &mut out_buffer);
            stream.shutdown(Both);
        }
        self.stopped = true;
    }

    fn next_token(&mut self) -> () {
        let next = self.token.0 + 1;
        self.token = Token(next);
    }

    fn send_buffer(stream: &mut TcpStream, buffer: &mut VecDeque<SerializedFrame>) -> io::Result<usize> {
        let mut sent_bytes = 0;
        let mut interrupts = 0;
        while let Some(mut buf) = buffer.pop_front() {
            match Self::write_serialized(stream, &mut buf) {
                Ok(n) => {
                    sent_bytes = sent_bytes + n;
                    match &mut buf {
                        // Split the data and continue sending the rest later
                        SerializedFrame::Bytes(bytes) => {
                            if n < bytes.len() {
                                bytes.split_to(n);
                                buffer.push_front(buf);
                            }
                        }
                        SerializedFrame::Chunk(chunk) => {
                            if n < chunk.bytes().len() {
                                chunk.advance(n);
                                buffer.push_front(buf);
                            }
                        }
                        _ => {}
                    }
                    // Continue looping for the next message
                }
                // Would block "errors" are the OS's way of saying that the
                // connection is not actually ready to perform this I/O operation.
                Err(ref err) if would_block(err) => {
                    // re-insert the data at the front of the buffer and return
                    buffer.push_front(buf);
                    return Ok(sent_bytes);
                }
                Err(err) if interrupted(&err) => {
                    // re-insert the data at the front of the buffer
                    buffer.push_front(buf);
                    interrupts += 1;
                    if interrupts >= MAX_INTERRUPTS {return Err(err)}
                }
                // Other errors we'll consider fatal.
                Err(err) => {
                    buffer.push_front(buf);
                    return Err(err);
                },
            }
        }
        return Ok(sent_bytes);
    }

    fn write_serialized(stream: &mut TcpStream, serialized: &mut SerializedFrame) -> io::Result<usize> {
        match serialized {
            SerializedFrame::Chunk(chunk) => {
                //println!("writing chunk");
                stream.write(chunk.bytes())
            }
            SerializedFrame::Bytes(bytes) => {
                //println!("writing bytes");
                stream.write(bytes.bytes())
            }
        }
    }
}

// Error handling helper functions
fn would_block(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::WouldBlock
}

fn interrupted(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::Interrupted
}

fn no_buffer_space(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::InvalidData
}

fn broken_pipe(err: &io::Error) -> bool {
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