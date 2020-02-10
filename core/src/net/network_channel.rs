use std::collections::VecDeque;
use mio::tcp::TcpStream;
use mio::{Token};
use std::net::SocketAddr;
use crate::messaging::Serialized;
use crate::net::buffer::{DecodeBuffer, BufferChunk};
use crate::net::{ConnectionState, network_thread};
use crate::net::frames::Frame;
use crate::net::network_thread::*;
use std::io;
use std::io::{Write, ErrorKind, Error};
use bytes::Buf;
use std::net::Shutdown::Both;

pub struct TcpChannel {
    stream: TcpStream,
    outbound_queue: VecDeque<Serialized>,
    pub token: Token,
    input_buffer: DecodeBuffer,
    pub state: ConnectionState,
}

impl TcpChannel {
    pub fn new(stream: TcpStream, token: Token, buffer_chunk: BufferChunk) -> Self {
        let mut input_buffer = DecodeBuffer::new(buffer_chunk);
        stream.set_nodelay(true);
        TcpChannel{
            stream,
            outbound_queue: VecDeque::new(),
            token,
            input_buffer,
            state: ConnectionState::New,
        }
    }

    pub fn stream(&self) -> &TcpStream {
        &self.stream
    }

    pub fn initialize(&mut self, addr: SocketAddr) -> () {

    }

    pub fn swap_buffer(&mut self, new_buffer: &mut BufferChunk) -> () {
        self.input_buffer.swap_buffer(new_buffer);
    }

    pub fn receive(&mut self) -> io::Result<usize> {
        let mut read_bytes = 0;
        let mut sum_read_bytes = 0;
        let mut interrupts = 0;
        loop {
            // Keep all the read bytes in the buffer without overwriting
            if read_bytes > 0 {
                self.input_buffer.advance_writeable(read_bytes);
            }
            if let Some(mut io_vec) = self.input_buffer.get_writeable() {
                match self.stream.read_bufs(&mut [&mut io_vec]) {
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
                        if interrupts >= network_thread::MAX_INTERRUPTS {
                            return Err(err)
                        }
                    },
                    Err(err) => {
                        println!("network thread {} got uncaught error during read from remote {}", self.stream.local_addr().unwrap(), self.stream.peer_addr().unwrap());
                        return Err(err)
                    }
                }
            } else {
                return Err(Error::new(ErrorKind::InvalidData, "No space in Buffer"));
            }
        }
    }

    pub fn shutdown(&mut self) -> () {
        self.stream.shutdown(Both);
    }

    pub fn decode(&mut self) -> Option<Frame> {
        self.input_buffer.get_frame()
    }

    pub fn enqueue_serialized(&mut self, serialized: Serialized) -> io::Result<usize> {
        self.outbound_queue.push_back(serialized);
        self.try_drain()
    }

    pub fn try_drain(&mut self) -> io::Result<usize> {
        let mut sent_bytes: usize = 0;
        let mut interrupts = 0;
        while let Some(mut serialized_frame) = self.outbound_queue.pop_front() {
            match self.write_serialized(&serialized_frame) {
                Ok(n) => {
                    sent_bytes = sent_bytes + n;
                    match &mut serialized_frame {
                        // Split the data and continue sending the rest later if we sent less than the full frame
                        Serialized::Bytes(bytes) => {
                            if n < bytes.len() {
                                bytes.split_to(n);
                                self.outbound_queue.push_front(serialized_frame);
                            }
                        }
                        Serialized::Chunk(chunk) => {
                            if n < chunk.bytes().len() {
                                chunk.advance(n);
                                self.outbound_queue.push_front(serialized_frame);
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
                    self.outbound_queue.push_front(serialized_frame);
                    return Ok(sent_bytes);
                }
                Err(err) if interrupted(&err) => {
                    // re-insert the data at the front of the buffer
                    self.outbound_queue.push_front(serialized_frame);
                    interrupts += 1;
                    if interrupts >= MAX_INTERRUPTS {return Err(err)}
                }
                // Other errors we'll consider fatal.
                Err(err) => {
                    self.outbound_queue.push_front(serialized_frame);
                    return Err(err);
                },
            }
        }
        Ok(sent_bytes)
    }

    pub fn write_serialized(&mut self, serialized: &Serialized) -> io::Result<usize> {
        match serialized {
            Serialized::Chunk(chunk) => {
                self.stream.write(chunk.bytes())
            }
            Serialized::Bytes(bytes) => {
                self.stream.write(bytes.bytes())
            }
        }
    }
}