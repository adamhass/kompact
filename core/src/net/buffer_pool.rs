use crate::net::buffer::{BufferChunk, Chunk, DefaultChunk};
use std::collections::VecDeque;

/// The number of Buffers each pool will Pre-allocate
pub const INITIAL_BUFFER_LEN: usize = 1;
const MAX_POOL_SIZE: usize = 10000;

/// Methods required by a ChunkAllocator
pub trait ChunkAllocator: Send + 'static {
    /// ChunkAllocators deliver Chunk by raw pointers
    fn get_chunk(&self) -> *mut dyn Chunk;
    /// This method tells the allocator that the Chunk may be de-allocated
    unsafe fn release(&self, ptr: *mut dyn Chunk) -> ();
}

/// A default allocator for Kompact
///
/// Heap allocates chunks through the normal Rust system allocator
#[derive(Default)]
pub(crate) struct DefaultAllocator {}

impl ChunkAllocator for DefaultAllocator {
    fn get_chunk(&self) -> *mut dyn Chunk {
        Box::into_raw(Box::new(DefaultChunk::new()))
    }

    unsafe fn release(&self, ptr: *mut dyn Chunk) -> () {
        Box::from_raw(ptr);
    }
}

pub(crate) struct BufferPool {
    pool: VecDeque<BufferChunk>,
    returned: VecDeque<BufferChunk>,
    pool_size: usize,
    chunk_allocator: Box<dyn ChunkAllocator>,
    reuse: bool,
    allocations: usize;
}

impl BufferPool {
    pub fn new(reuse: bool) -> Self {
        let mut pool = VecDeque::<BufferChunk>::new();
        let chunk_allocator = Box::new(DefaultAllocator::default());
        for _ in 0..INITIAL_BUFFER_LEN {
            pool.push_front(BufferChunk::from_chunk(chunk_allocator.get_chunk()));
        }
        BufferPool {
            pool,
            returned: VecDeque::new(),
            pool_size: INITIAL_BUFFER_LEN,
            chunk_allocator,
            reuse,
            allocations: INITIAL_BUFFER_LEN,
        }
    }

    /// Creates a BufferPool with a custom ChunkAllocator
    #[allow(dead_code)]
    pub fn with_allocator(chunk_allocator: Box<dyn ChunkAllocator>) -> Self {
        let mut pool = VecDeque::<BufferChunk>::new();
        for _ in 0..INITIAL_BUFFER_LEN {
            pool.push_front(BufferChunk::from_chunk(chunk_allocator.get_chunk()));
        }
        BufferPool {
            pool,
            returned: VecDeque::new(),
            pool_size: INITIAL_BUFFER_LEN,
            chunk_allocator,
            reuse: true,
            allocations: INITIAL_BUFFER_LEN,
        }
    }

    fn new_buffer(&mut self) -> BufferChunk {
        self.allocations += 1;
        eprintln!("allocating new buffer {}", self.allocations);
        BufferChunk::from_chunk(self.chunk_allocator.get_chunk())
    }

    pub fn get_buffer(&mut self) -> Option<BufferChunk> {
        if self.reuse {
            if let Some(new_buffer) = self.pool.pop_front() {
                return Some(new_buffer);
            }
            self.try_reclaim()
        } else {
            // Try to make sure the Buffer is allocated first, if the compiler or hardware figures out an optimization so be it...
            let mut new_buf = self.new_buffer();
            // This is a brutish GC method but it's sufficient for reasoning about performance.
            for _ in 0..2 { //
                if let Some(trash) = self.try_reclaim() {
                    std::mem::drop(trash);
                }
            }
            Some(new_buf)
        }
    }

    pub fn return_buffer(&mut self, buffer: BufferChunk) -> () {
        self.returned.push_back(buffer);
    }

    /// Iterates of returned buffers from oldest to newest trying to reclaim
    /// until it successfully finds an available one
    /// If it fails it will attempt to create a new buffer instead
    fn try_reclaim(&mut self) -> Option<BufferChunk> {
        for _i in 0..self.returned.len() {
            if let Some(mut returned_buffer) = self.returned.pop_front() {
                if returned_buffer.free() {
                    return Some(returned_buffer);
                } else {
                    self.returned.push_back(returned_buffer);
                }
            }
        }
        if !self.reuse {
            self.increase_pool()
        } else {None}
    }

    fn increase_pool(&mut self) -> Option<BufferChunk> {
        if self.pool_size >= MAX_POOL_SIZE {
            return None;
        };
        self.pool_size += 1;
        Some(self.new_buffer())
    }

    /// We use this method for assertions in tests
    #[allow(dead_code)]
    pub(crate) fn get_pool_sizes(&self) -> (usize, usize, usize) {
        (self.pool_size, self.pool.len(), self.returned.len())
    }
}
