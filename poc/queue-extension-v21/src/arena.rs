//! Spillover-arena allocator for variable-size envelope payloads.
//!
//! M1.2 (acct-q3nm). All operations run under exclusive
//! `PgLwLock<SpilloverArena>`; atomic ops use Relaxed because the
//! LWLock provides synchronization.
//!
//! ## Layout
//!
//! The arena's `bytes` array holds a linear sequence of blocks:
//!
//! ```text
//!   [block A header (8B)][block A payload (size_A bytes)]
//!   [block B header (8B)][block B payload (size_B bytes)]
//!   ...
//!   [unused — up to bump_offset]
//! ```
//!
//! Each block header is 8 bytes:
//! - `size: u32` — payload bytes (excludes header)
//! - `next_free: u32` — when block is on the freelist, offset of next free block's
//!   header (0 = end of list). When block is allocated, value is 0.
//!
//! ## Alloc strategy
//!
//! First-fit walk over the singly-linked freelist. On no fit, bump-allocate
//! from `bump_offset`. The freelist is LIFO — frees push to head, allocs
//! pop from head.
//!
//! ## What's deferred
//!
//! No coalesce on free. For workloads with consistent allocation sizes
//! (the bake-off shapes, see §5.2), pure freelist reuse keeps `bump_offset`
//! stable. If mixed-size workloads in soak tests surface monotone arena
//! growth, switch to a slab allocator per follow-up `acct-v21-fu-arena-fragmentation`
//! (spec §7 Q-H).
//!
//! ## Caller contract
//!
//! Callers track the (offset, requested_size) tuple themselves. `free`
//! reads the size from the block header but the caller usually has the
//! original allocation size from prior context (StagingEntry's
//! payload_length / sku_pool_count*8 / wip_pool_count*8). Misuse —
//! double-free, free with wrong offset — leaves the freelist corrupted.

use crate::SpilloverArena;
use std::sync::atomic::Ordering::Relaxed;

/// Per-block header. Read/written via raw pointer arithmetic into
/// `SpilloverArena.bytes`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct BlockHeader {
    size: u32,
    next_free: u32,
}

const BLOCK_HEADER_BYTES: u32 = 8;
/// Allocation alignment: 8 bytes. Cheap on 64-bit platforms and lets
/// us treat the freelist's `next_free` pointer as the first field of
/// a u64-aligned block.
const ALLOC_ALIGN: u32 = 8;

impl SpilloverArena {
    /// Allocate `size` bytes from the arena. Returns offset of the
    /// PAYLOAD (not the header). `size` is rounded up to 8-byte
    /// alignment. Returns `None` if the arena cannot satisfy the
    /// request.
    ///
    /// Walks the freelist for first-fit; if none, bumps from the
    /// high-water mark.
    pub fn alloc(&mut self, size: u32) -> Option<u32> {
        if size == 0 {
            return None;
        }
        let size = align_up(size, ALLOC_ALIGN);

        // Try freelist first.
        let mut prev_header_offset: u32 = 0;
        let mut cur_header_offset = self.freelist_head_offset.load(Relaxed);
        while cur_header_offset != 0 {
            let cur = self.read_header(cur_header_offset);
            if cur.size >= size {
                // Unlink from freelist.
                if prev_header_offset == 0 {
                    self.freelist_head_offset.store(cur.next_free, Relaxed);
                } else {
                    let mut prev = self.read_header(prev_header_offset);
                    prev.next_free = cur.next_free;
                    self.write_header(prev_header_offset, prev);
                }
                // Mark as allocated.
                self.write_header(
                    cur_header_offset,
                    BlockHeader { size: cur.size, next_free: 0 },
                );
                self.total_allocs.fetch_add(1, Relaxed);
                return Some(cur_header_offset + BLOCK_HEADER_BYTES);
            }
            prev_header_offset = cur_header_offset;
            cur_header_offset = cur.next_free;
        }

        // Bump-allocate from high-water mark.
        let header_offset = self.bump_offset.load(Relaxed);
        let needed = BLOCK_HEADER_BYTES + size;
        let new_bump = header_offset.checked_add(needed)?;
        if new_bump as usize > self.bytes.len() {
            return None;
        }
        self.bump_offset.store(new_bump, Relaxed);
        self.write_header(header_offset, BlockHeader { size, next_free: 0 });
        self.total_allocs.fetch_add(1, Relaxed);
        Some(header_offset + BLOCK_HEADER_BYTES)
    }

    /// Return a block to the freelist. `payload_offset` is the value
    /// previously returned by `alloc`. No coalesce.
    pub fn free(&mut self, payload_offset: u32) {
        let header_offset = payload_offset - BLOCK_HEADER_BYTES;
        let mut hdr = self.read_header(header_offset);
        let old_head = self.freelist_head_offset.load(Relaxed);
        hdr.next_free = old_head;
        self.write_header(header_offset, hdr);
        self.freelist_head_offset.store(header_offset, Relaxed);
        self.total_frees.fetch_add(1, Relaxed);
    }

    /// Write `data` to the arena at `payload_offset`. Caller must
    /// have allocated at least `data.len()` bytes at that offset.
    pub fn write_bytes(&mut self, payload_offset: u32, data: &[u8]) {
        let start = payload_offset as usize;
        let end = start + data.len();
        self.bytes[start..end].copy_from_slice(data);
    }

    /// Read `len` bytes from the arena at `payload_offset`.
    pub fn read_bytes(&self, payload_offset: u32, len: u32) -> Vec<u8> {
        let start = payload_offset as usize;
        let end = start + len as usize;
        self.bytes[start..end].to_vec()
    }

    /// Count free blocks (walks the freelist). O(n) — for tests + observability.
    pub fn freelist_count(&self) -> u32 {
        let mut count: u32 = 0;
        let mut cur = self.freelist_head_offset.load(Relaxed);
        while cur != 0 {
            let hdr = self.read_header(cur);
            count += 1;
            cur = hdr.next_free;
        }
        count
    }

    /// All-time total allocations.
    pub fn allocs_total(&self) -> u64 {
        self.total_allocs.load(Relaxed)
    }

    /// All-time total frees.
    pub fn frees_total(&self) -> u64 {
        self.total_frees.load(Relaxed)
    }

    /// Currently-outstanding allocations (allocs - frees). Should be 0
    /// when the system is at rest (no in-flight SuperBatches, all
    /// staging entries empty).
    pub fn outstanding_allocs(&self) -> u64 {
        self.allocs_total().saturating_sub(self.frees_total())
    }

    /// High-water mark — the maximum bytes ever bump-allocated.
    pub fn bump_offset_now(&self) -> u32 {
        self.bump_offset.load(Relaxed)
    }

    fn read_header(&self, header_offset: u32) -> BlockHeader {
        let off = header_offset as usize;
        let size = u32::from_le_bytes(self.bytes[off..off + 4].try_into().unwrap());
        let next_free = u32::from_le_bytes(self.bytes[off + 4..off + 8].try_into().unwrap());
        BlockHeader { size, next_free }
    }

    fn write_header(&mut self, header_offset: u32, hdr: BlockHeader) {
        let off = header_offset as usize;
        self.bytes[off..off + 4].copy_from_slice(&hdr.size.to_le_bytes());
        self.bytes[off + 4..off + 8].copy_from_slice(&hdr.next_free.to_le_bytes());
    }
}

fn align_up(value: u32, alignment: u32) -> u32 {
    (value + alignment - 1) & !(alignment - 1)
}
