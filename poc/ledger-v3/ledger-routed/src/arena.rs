//! Spillover-arena allocator. Verbatim port of
//! `poc/queue-extension-v21/src/arena.rs` (M1.2 / acct-q3nm) with the
//! `SpilloverArena` import path updated for v3's module layout.
//!
//! All operations run under exclusive `PgLwLock<SpilloverArena>`;
//! atomic ops use Relaxed because the LWLock provides
//! synchronization.
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
//! - `next_free: u32` — when block is on the freelist, offset of next
//!   free block's header (0 = end of list). When block is allocated,
//!   value is 0.
//!
//! ## Alloc strategy
//!
//! First-fit walk over the singly-linked freelist. On no fit, bump-
//! allocate from `bump_offset`. The freelist is LIFO — frees push to
//! head, allocs pop from head.
//!
//! ## What's deferred
//!
//! No coalesce on free. For workloads with consistent allocation
//! sizes (the §10.4 characterization scenarios), pure freelist reuse
//! keeps `bump_offset` stable. If mixed-size workloads in soak tests
//! surface monotone arena growth, switch to a slab allocator per
//! follow-up (v21's `acct-v21-fu-arena-fragmentation` shape).
//!
//! ## Caller contract
//!
//! Callers track the (offset, requested_size) tuple themselves.
//! `free` reads the size from the block header but the caller usually
//! has the original allocation size from prior context
//! (StagingEntry's `payload_length` / `pool_count * 8`). Misuse —
//! double-free, free with wrong offset — leaves the freelist
//! corrupted.

use crate::shmem::SpilloverArena;
use std::sync::atomic::Ordering::Relaxed;

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

#[allow(dead_code)] // Used by enqueue / router / committer modules as they land.
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

        // Latent v21 bug fix: offset 0 is the freelist-end sentinel
        // (`freelist_head_offset == 0` means "empty list"); but
        // `PGRXSharedMemory` zero-fills `bump_offset`, so the very
        // first bump-allocation puts a real block header at offset 0.
        // Freeing that block sets `freelist_head_offset == 0` which is
        // indistinguishable from empty — the block is lost, the
        // freelist walk skips it. v21 masks this under high churn (the
        // first-ever block rarely returns to the head). Lazy-init bumps
        // past offset 0 once so no real block header ever lives there.
        if self.bump_offset.load(Relaxed) == 0 {
            self.bump_offset.store(BLOCK_HEADER_BYTES, Relaxed);
        }

        let mut prev_header_offset: u32 = 0;
        let mut cur_header_offset = self.freelist_head_offset.load(Relaxed);
        while cur_header_offset != 0 {
            let cur = self.read_header(cur_header_offset);
            if cur.size >= size {
                if prev_header_offset == 0 {
                    self.freelist_head_offset.store(cur.next_free, Relaxed);
                } else {
                    let mut prev = self.read_header(prev_header_offset);
                    prev.next_free = cur.next_free;
                    self.write_header(prev_header_offset, prev);
                }
                self.write_header(
                    cur_header_offset,
                    BlockHeader {
                        size: cur.size,
                        next_free: 0,
                    },
                );
                self.total_allocs.fetch_add(1, Relaxed);
                return Some(cur_header_offset + BLOCK_HEADER_BYTES);
            }
            prev_header_offset = cur_header_offset;
            cur_header_offset = cur.next_free;
        }

        let header_offset = self.bump_offset.load(Relaxed);
        let needed = BLOCK_HEADER_BYTES + size;
        let new_bump = header_offset.checked_add(needed)?;
        if new_bump as usize > self.bytes.len() {
            return None;
        }
        self.bump_offset.store(new_bump, Relaxed);
        self.write_header(
            header_offset,
            BlockHeader { size, next_free: 0 },
        );
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

    /// Count free blocks (walks the freelist). O(n) — tests +
    /// observability.
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

    pub fn allocs_total(&self) -> u64 {
        self.total_allocs.load(Relaxed)
    }

    pub fn frees_total(&self) -> u64 {
        self.total_frees.load(Relaxed)
    }

    /// Currently-outstanding allocations (allocs - frees). Should be
    /// 0 when the system is at rest (no in-flight commit_groups, all
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

#[allow(dead_code)]
fn align_up(value: u32, alignment: u32) -> u32 {
    (value + alignment - 1) & !(alignment - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Heap-allocate a zero-initialized SpilloverArena. The struct
    /// is 128MB+; stack allocation would blow the test thread's
    /// 2MB default stack.
    fn fresh_arena() -> Box<SpilloverArena> {
        unsafe { Box::<SpilloverArena>::new_zeroed().assume_init() }
    }

    #[test]
    fn alloc_returns_distinct_offsets_and_bumps_high_water() {
        let mut a = fresh_arena();
        let o1 = a.alloc(64).unwrap();
        let o2 = a.alloc(64).unwrap();
        let o3 = a.alloc(64).unwrap();
        assert_ne!(o1, o2);
        assert_ne!(o2, o3);
        assert_eq!(a.allocs_total(), 3);
        assert_eq!(a.frees_total(), 0);
        assert_eq!(a.outstanding_allocs(), 3);
        assert!(a.bump_offset_now() >= 3 * (8 + 64));
    }

    #[test]
    fn alloc_zero_returns_none() {
        let mut a = fresh_arena();
        assert!(a.alloc(0).is_none());
        assert_eq!(a.allocs_total(), 0);
    }

    #[test]
    fn size_rounds_up_to_8_byte_alignment() {
        let mut a = fresh_arena();
        let _o1 = a.alloc(1).unwrap();
        let _o2 = a.alloc(7).unwrap();
        let _o3 = a.alloc(9).unwrap();
        // First alloc lazily bumps past offset 0 (BLOCK_HEADER_BYTES = 8)
        // to reserve the freelist-end sentinel. Each of the three allocs
        // consumes 8 (header) + aligned(size, 8) payload bytes:
        //   alloc(1) → 8 + 8 = 16
        //   alloc(7) → 8 + 8 = 16
        //   alloc(9) → 8 + 16 = 24
        // Total: 8 (sentinel) + 16 + 16 + 24 = 64.
        assert_eq!(a.bump_offset_now(), 8 + 16 + 16 + 24);
    }

    #[test]
    fn alloc_then_free_cycle_returns_outstanding_to_zero() {
        let mut a = fresh_arena();
        let offsets: Vec<u32> = (0..100).map(|_| a.alloc(64).unwrap()).collect();
        assert_eq!(a.outstanding_allocs(), 100);
        for o in &offsets {
            a.free(*o);
        }
        assert_eq!(a.outstanding_allocs(), 0);
        assert_eq!(a.allocs_total(), 100);
        assert_eq!(a.frees_total(), 100);
        assert_eq!(a.freelist_count(), 100);
    }

    #[test]
    fn freed_block_reused_before_bump() {
        let mut a = fresh_arena();
        let o1 = a.alloc(64).unwrap();
        let bump_before = a.bump_offset_now();
        a.free(o1);
        let o2 = a.alloc(64).unwrap();
        // o2 should reuse o1's slot (LIFO freelist head).
        assert_eq!(o1, o2);
        assert_eq!(a.bump_offset_now(), bump_before);
        assert_eq!(a.freelist_count(), 0);
    }

    #[test]
    fn first_fit_skips_too_small_block() {
        let mut a = fresh_arena();
        let small = a.alloc(8).unwrap();
        let large = a.alloc(128).unwrap();
        a.free(small); // pushes 8-byte block on freelist head
        a.free(large); // pushes 128-byte block on freelist head
        // Freelist head is the 128-byte block (LIFO); request 64
        // bytes — first-fit picks the head (128 >= 64).
        let reused = a.alloc(64).unwrap();
        assert_eq!(reused, large);
        assert_eq!(a.freelist_count(), 1);
    }

    #[test]
    fn first_fit_walks_past_head_when_head_too_small() {
        let mut a = fresh_arena();
        let large = a.alloc(128).unwrap();
        let small = a.alloc(8).unwrap();
        a.free(large); // head -> large(128)
        a.free(small); // head -> small(8) -> large(128)
        // Request 64 bytes; head (8) too small, walk to large (128) and reuse.
        let reused = a.alloc(64).unwrap();
        assert_eq!(reused, large);
        // 8-byte block remains on freelist.
        assert_eq!(a.freelist_count(), 1);
    }

    #[test]
    fn write_then_read_bytes_round_trips() {
        let mut a = fresh_arena();
        let o = a.alloc(32).unwrap();
        let payload = b"hello shmem arena world!";
        a.write_bytes(o, payload);
        let read = a.read_bytes(o, payload.len() as u32);
        assert_eq!(read, payload);
    }

    #[test]
    fn alloc_returns_none_when_arena_full() {
        let mut a = fresh_arena();
        // Request a single allocation larger than the arena.
        let too_big = (a.bytes.len() as u32) + 1024;
        assert!(a.alloc(too_big).is_none());
        assert_eq!(a.allocs_total(), 0);
    }

    #[test]
    fn stress_random_alloc_free_pattern_outstanding_zeros_at_rest() {
        // Deterministic mixed-size workload: alloc 500 random-sized
        // blocks, free half, alloc more, free the rest. At end,
        // outstanding must be 0; freelist contains every freed block.
        let mut a = fresh_arena();
        let sizes = [8u32, 16, 24, 32, 64, 128, 256, 512];
        let mut live: Vec<u32> = Vec::new();
        for i in 0..500 {
            let s = sizes[i % sizes.len()];
            live.push(a.alloc(s).expect("alloc"));
        }
        assert_eq!(a.outstanding_allocs(), 500);
        // Free half (every other).
        let mut keep = Vec::new();
        for (i, o) in live.into_iter().enumerate() {
            if i % 2 == 0 {
                a.free(o);
            } else {
                keep.push(o);
            }
        }
        assert_eq!(a.outstanding_allocs(), 250);
        // Alloc 100 more — should reuse from freelist where sizes fit.
        let bump_before = a.bump_offset_now();
        for _ in 0..100 {
            keep.push(a.alloc(32).expect("alloc 32"));
        }
        let bump_after = a.bump_offset_now();
        // Some reuse happened (freelist had 250 blocks; 100 fits should hit).
        assert!(bump_after - bump_before < 100 * (8 + 32));
        // Free everything.
        for o in keep {
            a.free(o);
        }
        assert_eq!(a.outstanding_allocs(), 0);
    }
}
