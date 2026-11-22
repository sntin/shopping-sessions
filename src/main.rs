// Shopping Sessions pipeline: CSV click-stream -> user_daily / campaign_daily Parquet.
// See rules.md for the exact semantics implemented here.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{ArrayRef, Date32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use hashbrown::{HashMap, HashTable};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;


#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

type Bytes = &'static [u8];

const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;
const P: usize = 256; // partitions

// ---------------------------------------------------------------- hashing

#[inline(always)]
fn hash_bytes(b: &[u8]) -> u64 {
    // FxHash-like over 8-byte words, then a final avalanche.
    const K: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut h: u64 = b.len() as u64;
    let mut chunks = b.chunks_exact(8);
    for c in &mut chunks {
        let w = u64::from_le_bytes(c.try_into().unwrap());
        h = (h.rotate_left(5) ^ w).wrapping_mul(K);
    }
    let r = chunks.remainder();
    if !r.is_empty() {
        let mut buf = [0u8; 8];
        buf[..r.len()].copy_from_slice(r);
        let w = u64::from_le_bytes(buf);
        h = (h.rotate_left(5) ^ w).wrapping_mul(K);
    }
    // murmur3 fmix64
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51afd7ed558ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
    h ^= h >> 33;
    h
}

#[inline(always)]
fn part_of(h: u64) -> usize {
    (h >> 40) as usize & (P - 1)
}

// ---------------------------------------------------------------- CSV dialect

/// Split one line into exactly N fields per the rules' dialect. Returns false if malformed.
#[inline(always)]
fn split_line<const N: usize>(line: Bytes, out: &mut [Bytes; N]) -> bool {
    let len = line.len();
    let mut k = 0usize;
    let mut start = 0usize;
    let mut quotes = 0u32;
    let mut i = 0usize;
    loop {
        if i == len || line[i] == b',' {
            if k == N {
                return false;
            }
            let mut f = &line[start..i];
            if quotes != 0 {
                let fl = f.len();
                if quotes != 2 || fl < 2 || f[0] != b'"' || f[fl - 1] != b'"' {
                    return false;
                }
                f = &f[1..fl - 1];
            }
            out[k] = f;
            k += 1;
            if i == len {
                break;
            }
            start = i + 1;
            quotes = 0;
        } else if line[i] == b'"' {
            quotes += 1;
        }
        i += 1;
    }
    k == N
}

/// Iterate the lines of a chunk (a run of complete lines; last may lack '\n').
#[inline(always)]
fn for_each_line(chunk: Bytes, mut f: impl FnMut(Bytes)) {
    let mut pos = 0usize;
    for nl in memchr::memchr_iter(b'\n', chunk) {
        f(&chunk[pos..nl]);
        pos = nl + 1;
    }
    if pos < chunk.len() {
        f(&chunk[pos..]);
    }
}


/// 64-byte block masks: (separator = ',' or '\n', quote, newline)
#[inline(always)]
fn block_masks(p: &[u8; 64]) -> (u64, u64, u64) {
    #[cfg(target_feature = "avx2")]
    unsafe {
        use std::arch::x86_64::*;
        let a = _mm256_loadu_si256(p.as_ptr() as *const __m256i);
        let b = _mm256_loadu_si256(p.as_ptr().add(32) as *const __m256i);
        let comma = _mm256_set1_epi8(b',' as i8);
        let quote = _mm256_set1_epi8(b'"' as i8);
        let nl = _mm256_set1_epi8(b'\n' as i8);
        let m = |x: __m256i, c: __m256i| _mm256_movemask_epi8(_mm256_cmpeq_epi8(x, c)) as u32 as u64;
        let c = m(a, comma) | (m(b, comma) << 32);
        let q = m(a, quote) | (m(b, quote) << 32);
        let n = m(a, nl) | (m(b, nl) << 32);
        (c | n, q, n)
    }
    #[cfg(not(target_feature = "avx2"))]
    {
        let (mut c, mut q, mut n) = (0u64, 0u64, 0u64);
        for i in 0..64 {
            let x = p[i];
            c |= ((x == b',') as u64) << i;
            q |= ((x == b'"') as u64) << i;
            n |= ((x == b'\n') as u64) << i;
        }
        (c | n, q, n)
    }
}

/// Scan a chunk of complete lines and call `f` with the N fields of every well-formed line.
///
/// Quote rule: a line is well formed iff every field that starts with `"` also ends with
/// `"` (length >= 2) and the line holds exactly two quotes per such field (so no other
/// field, and no quoted field's interior, has a quote).
#[inline(always)]
fn for_each_record<const N: usize>(chunk: Bytes, mut f: impl FnMut(&[Bytes; N])) {
    let n = chunk.len();
    let mut pos = [0u32; 16]; // separator positions of the current line
    let mut k = 0usize; // separators seen in current line (fields completed)
    let mut line_start = 0usize;
    let mut q_line_start = 0u32; // cumulative quotes before line start
    let mut q_cum = 0u32; // cumulative quotes before current block
    let mut base = 0usize;
    let mut tail = [0u8; 64];
    let mut emit = |line_start: usize, k: usize, pos: &[u32; 16], quotes: u32| {
        if k != N {
            return;
        }
        let mut fields: [Bytes; N] = [&[]; N];
        let mut st = line_start;
        if quotes == 0 {
            for i in 0..N {
                let e = pos[i] as usize;
                fields[i] = unsafe { chunk.get_unchecked(st..e) };
                st = e + 1;
            }
        } else {
            let mut qf = 0u32;
            let mut bad = 0u32;
            for i in 0..N {
                let e = pos[i] as usize;
                let len = e - st;
                let (first, last) = if len > 0 {
                    unsafe { (*chunk.get_unchecked(st), *chunk.get_unchecked(e - 1)) }
                } else {
                    (0, 0)
                };
                let q = (first == b'"') as usize;
                bad |= (q as u32) & ((len < 2) as u32 | (last != b'"') as u32);
                let s2 = st + q;
                let e2 = (e - q).max(s2);
                fields[i] = unsafe { chunk.get_unchecked(s2..e2) };
                qf += q as u32;
                st = e + 1;
            }
            if (bad != 0) | (quotes != 2 * qf) {
                return;
            }
        }
        f(&fields);
    };
    while base < n {
        let (sep, qm, nlm) = if base + 64 <= n {
            block_masks(unsafe { &*(chunk.as_ptr().add(base) as *const [u8; 64]) })
        } else {
            tail = [0u8; 64];
            tail[..n - base].copy_from_slice(&chunk[base..]);
            block_masks(&tail)
        };
        let mut m = sep;
        while m != 0 {
            let b = m.trailing_zeros();
            let p = base + b as usize;
            pos[k & 15] = p as u32 - 0; // offsets fit: chunks are < 4 GiB
            k += 1;
            if (nlm >> b) & 1 != 0 {
                let below = if b == 0 { 0 } else { qm & (u64::MAX >> (64 - b)) };
                let qnow = q_cum + below.count_ones();
                emit(line_start, k, &pos, qnow - q_line_start);
                k = 0;
                line_start = p + 1;
                q_line_start = qnow;
            }
            m &= m - 1;
        }
        q_cum += qm.count_ones();
        base += 64;
    }
    // last line without a trailing newline
    if line_start < n {
        pos[k & 15] = n as u32;
        k += 1;
        emit(line_start, k, &pos, q_cum - q_line_start);
    }
}

// ---------------------------------------------------------------- timestamps

#[inline(always)]
fn d2(s: &[u8], i: usize) -> Option<u32> {
    let a = s[i].wrapping_sub(b'0');
    let b = s[i + 1].wrapping_sub(b'0');
    if a < 10 && b < 10 { Some(a as u32 * 10 + b as u32) } else { None }
}

#[inline(always)]
const fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let m = m as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

const fn build_month_starts() -> [i32; 300 * 16] {
    let mut v = [0i32; 300 * 16];
    let mut y = 1900i64;
    while y < 2200 {
        let mut mo = 1u32;
        while mo <= 13 {
            let (yy, mm) = if mo == 13 { (y + 1, 1) } else { (y, mo) };
            v[((y - 1900) * 16) as usize + mo as usize] = days_from_civil(yy, mm, 1) as i32;
            mo += 1;
        }
        y += 1;
    }
    v
}
static MONTH_START: [i32; 300 * 16] = build_month_starts();

/// days since epoch of the 1st of month `mo` (1..=13, 13 = next January) at index (y-1900)*16+mo
#[inline(always)]
fn month_starts() -> &'static [i32; 300 * 16] {
    &MONTH_START
}

#[inline(always)]
fn is_leap(y: u32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

#[inline(always)]
fn ld8(s: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(s[i..i + 8].try_into().unwrap())
}

/// true if every byte of `x` selected by `mask` (0xFF per byte) is an ASCII digit,
/// where `d = x ^ 0x30..30` holds the digit values.
#[inline(always)]
fn all_digits(d: u64, mask: u64) -> bool {
    let hi = 0x8080_8080_8080_8080u64 & mask;
    ((d | d.wrapping_add(0x7676_7676_7676_7676)) & hi) == 0
}

#[inline(always)]
fn parse_datetime(s: &[u8]) -> Option<i64> {
    // YYYY-MM-DDTHH:MM:SS (first 19 bytes); caller guarantees len >= 20
    let a = ld8(s, 0); // YYYY-MM-
    let b = ld8(s, 8); // DDTHH:MM
    let c = ld8(s, 11); // HH:MM:SS
    // separators
    const SA: u64 = 0xFF00_00FF_0000_0000; // bytes 4 and 7
    const VA: u64 = (b'-' as u64) << 32 | (b'-' as u64) << 56;
    const SB: u64 = 0xFF << 16; // byte 2 of b = s[10]
    const VB: u64 = (b'T' as u64) << 16;
    const SC: u64 = 0xFF << 16 | 0xFF << 40; // bytes 2 and 5 of c
    const VC: u64 = (b':' as u64) << 16 | (b':' as u64) << 40;
    if (a & SA) != VA || (b & SB) != VB || (c & SC) != VC {
        return None;
    }
    const Z: u64 = 0x3030_3030_3030_3030;
    let da = a ^ Z;
    let db = b ^ Z;
    let dc = c ^ Z;
    if !all_digits(da, !SA) || !all_digits(db, 0xFFFF) || !all_digits(dc, !SC) {
        return None;
    }
    let by = |x: u64, i: u32| ((x >> (8 * i)) & 0xFF) as u32;
    let y = by(da, 0) * 1000 + by(da, 1) * 100 + by(da, 2) * 10 + by(da, 3);
    let mo = by(da, 5) * 10 + by(da, 6);
    let d = by(db, 0) * 10 + by(db, 1);
    let h = by(dc, 0) * 10 + by(dc, 1);
    let mi = by(dc, 3) * 10 + by(dc, 4);
    let se = by(dc, 6) * 10 + by(dc, 7);
    if mo < 1 || mo > 12 || d < 1 || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    let days = if (1900..2200).contains(&y) {
        let ms = month_starts();
        let i = ((y - 1900) * 16 + mo) as usize;
        let (m0, m1) = unsafe { (*ms.get_unchecked(i), *ms.get_unchecked(i + 1)) };
        if d as i32 > m1 - m0 {
            return None;
        }
        m0 as i64 + d as i64 - 1
    } else {
        let dim = match mo {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            _ => {
                if is_leap(y) { 29 } else { 28 }
            }
        };
        if d > dim {
            return None;
        }
        days_from_civil(y as i64, mo, d)
    };
    Some(((days * 24 + h as i64) * 60 + mi as i64) * 60_000 + se as i64 * 1000)
}

/// ingest_ts is always a valid F1 timestamp: parse it without validation (falls back to
/// the full parser if the shape is unexpected). Returns epoch seconds.
#[inline(always)]
fn parse_ingest_secs(s: &[u8]) -> Option<i64> {
    if s.len() != 20 {
        return parse_ts(s).map(|t| t.div_euclid(1000));
    }
    let a = ld8(s, 0) ^ 0x3030_3030_3030_3030; // YYYY-MM-
    let b = ld8(s, 8) ^ 0x3030_3030_3030_3030; // DDTHH:MM
    let c = ld8(s, 11) ^ 0x3030_3030_3030_3030; // HH:MM:SS
    let by = |x: u64, i: u32| ((x >> (8 * i)) & 0xFF) as u32;
    let y = by(a, 0) * 1000 + by(a, 1) * 100 + by(a, 2) * 10 + by(a, 3);
    let mo = by(a, 5) * 10 + by(a, 6);
    let d = by(b, 0) * 10 + by(b, 1);
    let h = by(c, 0) * 10 + by(c, 1);
    let mi = by(c, 3) * 10 + by(c, 4);
    let se = by(c, 6) * 10 + by(c, 7);
    if !(1900..2200).contains(&y) || mo < 1 || mo > 12 {
        return parse_ts(s).map(|t| t.div_euclid(1000));
    }
    let days = month_starts()[((y - 1900) * 16 + mo) as usize] as i64 + d as i64 - 1;
    Some(((days * 24 + h as i64) * 60 + mi as i64) * 60 + se as i64)
}

/// Parse 8 ASCII digits (first digit in the lowest byte) to their value.
#[inline(always)]
fn parse8(x: u64) -> u64 {
    let x = x & 0x0F0F_0F0F_0F0F_0F0F;
    let x = (x.wrapping_mul(10 << 8 | 1)) >> 8 & 0x00FF_00FF_00FF_00FF;
    let x = (x.wrapping_mul(100 << 16 | 1)) >> 16 & 0x0000_FFFF_0000_FFFF;
    (x.wrapping_mul(10000 << 32 | 1)) >> 32
}

/// Parse an event/click ts to epoch ms. None = bad timestamp.
#[inline(always)]
fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 13 {
        // F3 fast path: 13 digits
        const Z: u64 = 0x3030_3030_3030_3030;
        let a = ld8(s, 0) ^ Z;
        let b = ((ld8(s, 5) ^ Z) >> 24) << 24; // digits 8..13 in the top 5 bytes, zeros below
        if !all_digits(a, u64::MAX) || !all_digits(b, u64::MAX) {
            return None;
        }
        return Some((parse8(a) * 100_000 + parse8(b)) as i64);
    }
    if (n == 20 || n == 25) && s[10] == b'T' {
        let t = parse_datetime(s)?;
        if n == 20 {
            return if s[19] == b'Z' { Some(t) } else { None };
        }
        let sign = s[19];
        if (sign != b'+' && sign != b'-') || s[22] != b':' {
            return None;
        }
        let oh = d2(s, 20)?;
        let om = d2(s, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh as i64 * 60 + om as i64) * 60_000;
        return Some(if sign == b'+' { t - off } else { t + off });
    }
    // F3: ASCII digits only
    if n == 0 {
        return None;
    }
    let mut v: u64 = 0;
    let mut i = 0;
    while i < n && s[i] == b'0' {
        i += 1;
    }
    let mut sig = 0;
    while i < n {
        let c = s[i].wrapping_sub(b'0');
        if c >= 10 {
            return None;
        }
        sig += 1;
        if sig > 19 {
            // too large for i64 (or at least far outside any window); still must be all digits
            for &b in &s[i..] {
                if b.wrapping_sub(b'0') >= 10 {
                    return None;
                }
            }
            return Some(i64::MAX);
        }
        v = v * 10 + c as u64;
        i += 1;
    }
    if v > i64::MAX as u64 { Some(i64::MAX) } else { Some(v as i64) }
}

#[inline(always)]
fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &b in s {
        let c = b.wrapping_sub(b'0');
        if c >= 10 {
            return None;
        }
        v = v * 10 + c as u64;
        if v > 10_000_000 {
            // keep validating digits? any further digit keeps it > 10M unless leading zeros, which can't follow non-zero
            for &b2 in s {
                if b2.wrapping_sub(b'0') >= 10 {
                    return None;
                }
            }
            return None;
        }
    }
    if v >= 1 { Some(v as u32) } else { None }
}

// ---------------------------------------------------------------- inputs

fn leak_mmap(path: &Path) -> Bytes {
    let f = fs::File::open(path).unwrap_or_else(|e| panic!("open {:?}: {}", path, e));
    let len = f.metadata().map(|m| m.len()).unwrap_or(0);
    if len == 0 {
        return &[];
    }
    let m = unsafe { memmap2::MmapOptions::new().map(&f) }.expect("mmap");
    let m = Box::leak(Box::new(m));
    &m[..]
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v = Vec::new();
    if let Ok(rd) = fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file() {
                v.push(p);
            }
        }
    }
    v.sort();
    v
}

/// Split files into chunks of complete lines (headers removed).
/// Fault a chunk's pages in with one syscall (MADV_POPULATE_READ) instead of one fault per 64 KiB.
fn populate(c: Bytes) {
    if c.is_empty() || std::env::var_os("SS_NOPOP").is_some() {
        return;
    }
    let a = c.as_ptr() as usize & !4095;
    let e = c.as_ptr() as usize + c.len();
    // Only when the file is already in the page cache: on a cold cache, populating
    // serializes the reads and loses to readahead overlapping with parsing.
    let np = (e - a + 4095) / 4096;
    let mut v = vec![0u8; np];
    let r = unsafe { libc::mincore(a as *mut libc::c_void, e - a, v.as_mut_ptr()) };
    if r != 0 || v.iter().any(|&x| x & 1 == 0) {
        return;
    }
    unsafe { libc::madvise(a as *mut libc::c_void, e - a, 22) };
}
fn chunks_of(files: &[PathBuf], target: usize) -> Vec<Bytes> {
    let datas: Vec<Bytes> = files.par_iter().map(|p| leak_mmap(p)).collect();
    let mut out = Vec::new();
    for data in datas {
        let body = match memchr::memchr(b'\n', data) {
            Some(i) => &data[i + 1..],
            None => continue,
        };
        let mut s = 0;
        while s < body.len() {
            let mut e = (s + target).min(body.len());
            if e < body.len() {
                e = match memchr::memchr(b'\n', &body[e..]) {
                    Some(k) => e + k + 1,
                    None => body.len(),
                };
            }
            out.push(&body[s..e]);
            s = e;
        }
    }
    out
}

fn read_small_table(path: &Path) -> Vec<(Bytes, Bytes)> {
    let data = leak_mmap(path);
    let body = match memchr::memchr(b'\n', data) {
        Some(i) => &data[i + 1..],
        None => return Vec::new(),
    };
    let mut v = Vec::new();
    for_each_line(body, |line| {
        let mut f: [Bytes; 2] = [&[]; 2];
        if split_line::<2>(line, &mut f) && !f[0].is_empty() {
            v.push((f[0], f[1]));
        }
    });
    v
}

fn json_str(s: &str, key: &str) -> String {
    let k = format!("\"{}\"", key);
    let i = s.find(&k).expect("manifest key") + k.len();
    let rest = &s[i..];
    let a = rest.find('"').unwrap() + 1;
    let b = rest[a..].find('"').unwrap() + a;
    rest[a..b].to_string()
}

// ---------------------------------------------------------------- records

/// 128-bit identity of a string (xxh3-128). Collisions among a few million IDs are ~1e-25.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
struct H2 {
    lo: u64,
    hi: u64,
}

#[inline(always)]
fn h2(b: &[u8]) -> H2 {
    let v = xxhash_rust::xxh3::xxh3_128(b);
    H2 { lo: v as u64, hi: (v >> 64) as u64 }
}

/// 64-bit identity of a user ID / person name (xxh3-64).
#[inline(always)]
fn h1(b: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(b)
}

#[inline(always)]
fn part1(h: u64) -> usize {
    (h >> 40) as usize & (P - 1)
}

/// Person key: the name itself when it is 1-10 characters of `-0-9A-Za-z_` (6 bits each,
/// length in the low nibble; exact and invertible), otherwise a 60-bit hash of the name with
/// low nibble 0xF. Person records then need no name pointer: short names are decoded from
/// the key and long ones come from a side list.
const LONGK: u64 = 0xF;

const fn build_code() -> [u8; 256] {
    let mut t = [0xFFu8; 256];
    t[b'-' as usize] = 0;
    let mut i = 0;
    while i < 10 {
        t[b'0' as usize + i] = 1 + i as u8;
        i += 1;
    }
    i = 0;
    while i < 26 {
        t[b'A' as usize + i] = 11 + i as u8;
        t[b'a' as usize + i] = 38 + i as u8;
        i += 1;
    }
    t[b'_' as usize] = 37;
    t
}
static CODE: [u8; 256] = build_code();

const fn build_decode() -> [u8; 64] {
    let c = build_code();
    let mut d = [0u8; 64];
    let mut i = 0;
    while i < 256 {
        if c[i] != 0xFF {
            d[c[i] as usize] = i as u8;
        }
        i += 1;
    }
    d
}
static DECODE: [u8; 64] = build_decode();

#[inline(always)]
fn pkey(b: &[u8]) -> u64 {
    let n = b.len();
    if (1..=10).contains(&n) {
        let mut k = 0u64;
        let mut bad = 0u8;
        for (i, &c) in b.iter().enumerate() {
            let x = CODE[c as usize];
            bad |= x;
            k |= ((x & 63) as u64) << (58 - 6 * i);
        }
        if bad & 0xC0 == 0 {
            return k | n as u64;
        }
    }
    (h1(b) & !0xF) | LONGK
}

/// Decode a short person key into `out`.
fn decode_key(k: u64, out: &mut Vec<u8>) {
    let n = (k & 0xF) as usize;
    for i in 0..n {
        out.push(DECODE[((k >> (58 - 6 * i)) & 63) as usize]);
    }
}

#[inline(always)]
fn mixk(k: u64) -> u64 {
    let x = (k ^ (k >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    let x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// Person partition of a person key.
#[inline(always)]
fn partk(k: u64) -> usize {
    (mixk(k) >> 56) as usize & (P - 1)
}

/// Hash of a person key for tables inside one partition (independent of the partition bits).
#[inline(always)]
fn hashk(k: u64) -> u64 {
    mixk(k ^ 0x9E37_79B9_7F4A_7C15)
}

#[inline(always)]
fn part2(h: H2) -> usize {
    (h.hi >> 40) as usize & (P - 1)
}

/// Capacity for one partition's share of `n` records: mean + ~3 sigma, so few vectors regrow
/// and little untouched slack gets zeroed by the kernel.
fn part_cap(n: usize) -> usize {
    let m = n / P;
    m + 3 * (m as f64).sqrt() as usize + 8
}

/// Pack a &'static [u8] into a u64 (48-bit pointer, 16-bit length).
#[inline(always)]
fn pack(b: Bytes) -> u64 {
    debug_assert!(b.len() < 65536);
    (b.as_ptr() as u64) | ((b.len() as u64) << 48)
}

#[inline(always)]
fn unpack(x: u64) -> Bytes {
    unsafe { std::slice::from_raw_parts((x & 0xFFFF_FFFF_FFFF) as *const u8, (x >> 48) as usize) }
}

const INVALID: u32 = u32::MAX;

#[derive(Clone, Copy)]
struct PEv {
    pk: u64,  // person key
    rel: u32, // ts - (ws - 7d)
    pc: u32,
}

/// A click in the person stage: ts << 32 | campaign, and the packed click_id.
#[derive(Clone, Copy)]
struct Ck {
    k: u64,
    cid: u64,
}

#[derive(Clone, Copy)]
struct PCl {
    ph: u64,
    cid: u64,
    rel: u32,
    camp: u16,
}

struct Ctx {
    ws: i64,
    we: i64,
    base: i64, // ws - 7d
    products: IdMap,
    unknown_cat: u16,
    campaigns: IdMap,
}

/// Exact string -> u16 map for the small reference tables. Keys of 1 to 7 bytes are
/// packed into a u64 (bytes + length in the top byte) and found in an open-addressing
/// table with no string compare; longer keys use a regular hash map.
struct IdMap {
    keys: Vec<u64>, // 0 = empty
    vals: Vec<u16>,
    shift: u32,
    long: HashMap<Bytes, u16>,
}

impl IdMap {
    fn new(n: usize) -> Self {
        let size = (n * 4).next_power_of_two().max(64);
        IdMap { keys: vec![0; size], vals: vec![0; size], shift: 64 - size.trailing_zeros(), long: HashMap::new() }
    }
    /// Packed key of a short string (len <= 7), None for longer ones.
    #[inline(always)]
    fn key(b: &[u8]) -> Option<u64> {
        let n = b.len();
        if n > 7 || n == 0 {
            return None;
        }
        let p = b.as_ptr();
        // 8-byte load when it stays inside the page (mapped memory), else copy
        let x = if (p as usize & 4095) <= 4088 {
            unsafe { std::ptr::read_unaligned(p as *const u64) }
        } else {
            let mut t = [0u8; 8];
            t[..n].copy_from_slice(b);
            u64::from_le_bytes(t)
        };
        let x = x & (u64::MAX >> (64 - 8 * n as u32));
        Some(x | (n as u64) << 56)
    }
    #[inline(always)]
    fn slot(&self, k: u64) -> usize {
        (k.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> self.shift) as usize
    }
    /// Insert if absent; returns false if the key was already present.
    fn insert(&mut self, b: Bytes, v: u16) -> bool {
        match Self::key(b) {
            None => {
                if self.long.contains_key(b) {
                    return false;
                }
                self.long.insert(b, v);
                true
            }
            Some(k) => {
                let mask = self.keys.len() - 1;
                let mut i = self.slot(k);
                loop {
                    if self.keys[i] == k {
                        return false;
                    }
                    if self.keys[i] == 0 {
                        self.keys[i] = k;
                        self.vals[i] = v;
                        return true;
                    }
                    i = (i + 1) & mask;
                }
            }
        }
    }
    #[inline(always)]
    fn get(&self, b: &[u8]) -> Option<u16> {
        match Self::key(b) {
            None => self.long.get(b).copied(),
            Some(k) => {
                let mask = self.keys.len() - 1;
                let mut i = self.slot(k);
                loop {
                    let e = unsafe { *self.keys.get_unchecked(i) };
                    if e == k {
                        return Some(unsafe { *self.vals.get_unchecked(i) });
                    }
                    if e == 0 {
                        return None;
                    }
                    i = (i + 1) & mask;
                }
            }
        }
    }
}

fn main() {
    let t0 = Instant::now();
    // THP on (default) is ~8% faster with mimalloc purge disabled; SS_NOTHP=1 turns it off.
    if std::env::var("SS_NOTHP").is_ok() {
        unsafe { libc::prctl(libc::PR_SET_THP_DISABLE, 1, 0, 0, 0) };
    }
    let data_dir = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out_dir = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let ncpu: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8));
    let env_n = |k: &str| std::env::var(k).ok().and_then(|v| v.parse::<usize>().ok());
    rayon::ThreadPoolBuilder::new().num_threads(env_n("SS_THREADS").unwrap_or(ncpu).max(1)).build_global().unwrap();

    let manifest = fs::read_to_string(data_dir.join("manifest.json")).expect("manifest");
    let ws = parse_ts(json_str(&manifest, "window_start").as_bytes()).expect("window_start");
    let we = parse_ts(json_str(&manifest, "window_end").as_bytes()).expect("window_end");
    let base = ws - WEEK;

    // products / categories
    let mut cat_names: Vec<Bytes> = Vec::new();
    let mut cat_idx: HashMap<Bytes, u16> = HashMap::new();
    let ptab = read_small_table(&data_dir.join("products.csv"));
    let mut products = IdMap::new(ptab.len());
    for (pid, cat) in ptab {
        let c = *cat_idx.entry(cat).or_insert_with(|| {
            cat_names.push(cat);
            (cat_names.len() - 1) as u16
        });
        products.insert(pid, c);
    }
    let unknown_cat = cat_names.len() as u16;
    assert!(unknown_cat < 256, "more than 255 categories");
    cat_names.push(b"unknown");
    // rank of each category by byte order (for tie-breaks)
    let mut order: Vec<u16> = (0..cat_names.len() as u16).collect();
    order.sort_by(|&a, &b| cat_names[a as usize].cmp(cat_names[b as usize]));
    let mut cat_rank = vec![0u16; cat_names.len()];
    for (r, &c) in order.iter().enumerate() {
        cat_rank[c as usize] = r as u16;
    }

    // campaigns
    let mut camp_ids: Vec<Bytes> = Vec::new();
    let mut camp_chan: Vec<Bytes> = Vec::new();
    let ctab = read_small_table(&data_dir.join("campaigns.csv"));
    let mut campaigns = IdMap::new(ctab.len());
    for (cid, ch) in ctab {
        if campaigns.insert(cid, camp_ids.len() as u16) {
            camp_ids.push(cid);
            camp_chan.push(ch);
        }
    }
    let ncamp = camp_ids.len();
    let organic = ncamp; // index
    let ctx = Ctx { ws, we, base, products, unknown_cat, campaigns };

    let ev_files = list_csv(&data_dir.join("events"));
    let cl_files = list_csv(&data_dir.join("clicks"));
    let ln_files = list_csv(&data_dir.join("links"));
    eprintln!("[{:?}] setup", t0.elapsed());

    // ---- stage 1 (concurrently): links -> name map, clicks parse; events parse + dedup
    // links run on a dedicated thread (small input, sequential union-find) while all
    // pool threads parse events.
    let ln_files2 = ln_files.clone();
    let build = move || {
        let lm = if std::env::var_os("SS_OLDLINKS").is_some() { build_links(&ln_files2) } else { build_links_par(&ln_files2) };
        eprintln!("[{:?}] links done ({} remapped ids)", t0.elapsed(), lm.len);
        let _ = LINKS.set(lm);
    };
    // Links build on their own small pool while all main pool threads parse events;
    // chunks parsed before the link map is ready wait in a side buffer.
    let nl = env_n("SS_LINKTHREADS").unwrap_or(ncpu / 2).max(1);
    let lt = std::thread::spawn(move || {
        rayon::ThreadPoolBuilder::new().num_threads(nl).build().unwrap().install(build);
    });
    let ev_chunks = parse_events(&ev_files, &ctx);
    lt.join().unwrap();
    eprintln!("[{:?}] events parsed", t0.elapsed());
    let linkmap = LINKS.get().unwrap();
    let click_parts = parse_clicks(&cl_files, &ctx, &linkmap);
    eprintln!("[{:?}] clicks parsed", t0.elapsed());

    // ---- stage 2: dedup each event partition; losers are marked per person partition
    let lost_lists: Vec<Vec<u32>> = (0..P).into_par_iter().map(|q| dedup_partition(q, &ev_chunks)).collect();
    let nchunks = ev_chunks.len();
    let mut lost_by_p: Vec<Vec<u32>> = vec![Vec::new(); P];
    for l in &lost_lists {
        for &loc in l {
            lost_by_p[(loc >> 14 & 0xFF) as usize].push(loc);
        }
    }
    drop(lost_lists);
    let mut ev_long: Vec<Vec<Vec<(u64, u64)>>> = Vec::with_capacity(ev_chunks.len());
    let ev_person: Vec<Vec<Vec<PEv>>> = ev_chunks
        .into_iter()
        .map(|c| {
            ev_long.push(c.long);
            c.pev
        })
        .collect();
    let _ = nchunks;
    eprintln!("[{:?}] dedup done", t0.elapsed());

    // ---- stage 3: per person partition: sessionize, attribute, aggregate
    let ndays = ((we - 1).div_euclid(DAY) - ws.div_euclid(DAY) + 1).max(1) as usize;
    let ws_day = ws.div_euclid(DAY);
    let user_dir = out_dir.join("user_daily");
    let camp_dir = out_dir.join("campaign_daily");
    fs::create_dir_all(&user_dir).unwrap();
    fs::create_dir_all(&camp_dir).unwrap();

    let cat_names_s: Vec<String> = cat_names.iter().map(|b| String::from_utf8_lossy(b).into_owned()).collect();

    // partitions per task (power of two dividing P)
    let group: usize = env_n("SS_GROUP").filter(|g| g.is_power_of_two() && *g <= P).unwrap_or(4);
    let csize = (ncamp + 1) * ndays * 4;
    // One worker per pool thread takes tasks from a shared counter and keeps its own
    // campaign totals and user rows; each worker writes one user_daily file, so the
    // file count is the thread count, not the task count.
    let ntasks = P / group;
    let next = std::sync::atomic::AtomicUsize::new(0);
    let nworkers = rayon::current_num_threads().clamp(1, ntasks);
    let accs: Vec<Vec<u64>> = (0..nworkers)
        .into_par_iter()
        .map(|w| {
            let mut cacc = vec![0u64; csize];
            let mut rows = UserRows::default();
            let mut stamp = vec![u32::MAX; ncamp + 1];
            let mut stamp_ctr = 0u32;
            loop {
                let g = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if g >= ntasks {
                    break;
                }
                for p in g * group..(g + 1) * group {
                    process_person_partition(
                        p,
                        &ev_person,
                        &ev_long,
                        &lost_by_p[p],
                        &click_parts,
                        ws_day,
                        base,
                        ndays,
                        organic,
                        &cat_rank,
                        &mut cacc,
                        &mut rows,
                        &mut stamp,
                        &mut stamp_ctr,
                    );
                }
            }
            if !rows.day.is_empty() || w == 0 {
                write_user_rows(&user_dir.join(format!("part-{:05}.parquet", w)), &rows, &cat_names_s);
            }
            cacc
        })
        .collect();
    let mut camp_acc = vec![0u64; csize];
    for a in &accs {
        for (x, y) in camp_acc.iter_mut().zip(a.iter()) {
            *x += *y;
        }
    }
    drop(accs);
    eprintln!("[{:?}] sessions + user_daily written", t0.elapsed());

    write_campaign(&camp_dir.join("part-00000.parquet"), &camp_acc, ncamp, ndays, ws_day, &camp_ids, &camp_chan);
    eprintln!("[{:?}] done", t0.elapsed());
    if let Ok(st) = fs::read_to_string("/proc/self/status") {
        for l in st.lines().filter(|l| l.starts_with("VmHWM") || l.starts_with("RssAnon") || l.starts_with("RssFile")) {
            eprintln!("{}", l);
        }
    }
    // Skip destructors of huge structures.
    std::process::exit(0);
}

// ---------------------------------------------------------------- links

/// Open-addressing table: ID hash -> person (name hash, name pointer). Only IDs whose
/// person name differs from themselves are stored; a miss means "named by itself".
#[derive(Clone, Copy, Default)]
struct LinkSlot {
    h: u64,
    name_h: u64,
    name_ptr: u64,
}

struct LinkMap {
    slots: Vec<LinkSlot>,
    mask: usize,
    len: usize,
}

impl LinkMap {
    #[inline(always)]
    fn prefetch(&self, h: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::*;
            let p = self.slots.as_ptr().add(h as usize & self.mask) as *const i8;
            _mm_prefetch(p, _MM_HINT_T0);
            _mm_prefetch(p.add(40), _MM_HINT_T0);
        }
    }
    /// Resolve an ID hash to its person (key, name pointer); None = named by itself.
    #[inline(always)]
    fn resolve(&self, h: u64) -> Option<(u64, u64)> {
        let mut i = h as usize & self.mask;
        loop {
            let s = unsafe { self.slots.get_unchecked(i) };
            if s.h == h {
                return Some((s.name_h, s.name_ptr));
            }
            if s.h == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }

    fn build(entries: impl Iterator<Item = LinkSlot>, n: usize) -> LinkMap {
        let cap = (n * 2).next_power_of_two().max(16);
        let mask = cap - 1;
        let mut slots = vec![LinkSlot::default(); cap];
        let mut len = 0;
        for e in entries {
            let mut i = e.h as usize & mask;
            while slots[i].h != 0 {
                i = (i + 1) & mask;
            }
            slots[i] = e;
            len += 1;
        }
        LinkMap { slots, mask, len }
    }
}

/// Big-endian 8-byte prefix: comparing (prefix, then full bytes on a tie) is byte-wise order.
#[inline(always)]
fn name_pfx(b: &[u8]) -> u64 {
    let mut x = [0u8; 8];
    let n = b.len().min(8);
    x[..n].copy_from_slice(&b[..n]);
    u64::from_be_bytes(x)
}

#[inline(always)]
fn name_lt_eq(pa: u64, a: u64, pb: u64, b: u64) -> bool {
    if pa != pb {
        return pa < pb;
    }
    unpack(a) <= unpack(b)
}

/// A Vec of n zero-initialized T (T must be valid as all-zero bytes). Fresh allocator
/// memory is already zero, so this avoids touching every page up front.
fn zeroed_vec<T>(n: usize) -> Vec<T> {
    if n == 0 {
        return Vec::new();
    }
    let layout = std::alloc::Layout::array::<T>(n).unwrap();
    unsafe {
        let p = std::alloc::alloc_zeroed(layout) as *mut T;
        assert!(!p.is_null());
        Vec::from_raw_parts(p, n, n)
    }
}

/// Parallel link building (all pool threads): concurrent intern table, lock-free
/// union-find where a root is always the byte-wise smallest name of its set, and a
/// parallel fill of the link map.
fn build_links_par(files: &[PathBuf]) -> LinkMap {
    use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering::*};

    let chunks = chunks_of(files, 1 << 20);
    let pairs: Vec<Vec<(u64, u64, u64, u64, u64, u64)>> = chunks
        .par_iter()
        .map(|c| {
            let mut v = Vec::with_capacity(c.len() / 20);
            for_each_record::<2>(c, |f| {
                if !f[0].is_empty() && !f[1].is_empty() {
                    v.push((h1(f[0]).max(1), pack(f[0]), name_pfx(f[0]), h1(f[1]).max(1), pack(f[1]), name_pfx(f[1])));
                }
            });
            v
        })
        .collect();
    let total: usize = pairs.iter().map(|v| v.len()).sum();
    if total == 0 {
        return LinkMap::build(std::iter::empty(), 0);
    }
    // intern: one packed slot per entry, (hash >> 24) << 24 | (id + 1); the full hash is
    // checked in id_h. Ids come from a counter; a lost race leaves an unused id (a hole).
    let cap = (total * 3).next_power_of_two();
    let mask = cap - 1;
    let slots_t: Vec<AtomicU64> = zeroed_vec(cap);
    const IDB: usize = 1024; // ids handed out in blocks per task
    let maxn = total * 2 + (pairs.len() + 1) * IDB;
    assert!(maxn < (1 << 24));
    let id_h: Vec<AtomicU64> = zeroed_vec(maxn);
    let id_ptr: Vec<AtomicU64> = zeroed_vec(maxn);
    let id_pfx: Vec<AtomicU64> = zeroed_vec(maxn);
    let next = AtomicUsize::new(0);
    let intern = |h: u64, ptr: u64, pfx: u64, blk: &mut (usize, usize)| -> u32 {
        let tag = (h >> 24) << 24;
        let mut i = (h >> 20) as usize & mask;
        let mut mine: Option<usize> = None;
        loop {
            let e = slots_t[i].load(Acquire);
            if e == 0 {
                let id = *mine.get_or_insert_with(|| {
                    if blk.0 == blk.1 {
                        let b = next.fetch_add(IDB, Relaxed);
                        *blk = (b, b + IDB);
                    }
                    let id = blk.0;
                    blk.0 += 1;
                    id_h[id].store(h, Relaxed);
                    id_ptr[id].store(ptr, Relaxed);
                    id_pfx[id].store(pfx, Relaxed);
                    id
                });
                match slots_t[i].compare_exchange(0, tag | (id as u64 + 1), AcqRel, Acquire) {
                    Ok(_) => return id as u32,
                    Err(_) => continue, // re-examine this slot
                }
            }
            if e & !0xFF_FFFF == tag {
                let id = (e & 0xFF_FFFF) as usize - 1;
                if id_h[id].load(Relaxed) == h {
                    if let Some(m) = mine {
                        id_h[m].store(0, Relaxed); // hole
                    }
                    return id as u32;
                }
            }
            i = (i + 1) & mask;
        }
    };
    let prefetch = |h: u64| unsafe {
        std::arch::x86_64::_mm_prefetch(slots_t.as_ptr().add((h >> 20) as usize & mask) as *const i8, std::arch::x86_64::_MM_HINT_T0);
    };
    let idpairs: Vec<Vec<(u32, u32)>> = pairs
        .par_iter()
        .map(|v| {
            let mut out = Vec::with_capacity(v.len());
            let mut blk = (0usize, 0usize);
            for k in 0..v.len() {
                if k + 8 < v.len() {
                    prefetch(v[k + 8].0);
                    prefetch(v[k + 8].3);
                }
                let (ha, pa, xa, hb, pb, xb) = v[k];
                out.push((intern(ha, pa, xa, &mut blk), intern(hb, pb, xb, &mut blk)));
            }
            out
        })
        .collect();
    drop(slots_t);
    let n = next.load(SeqCst);
    let id_h: Vec<u64> = id_h.into_iter().take(n).map(|x| x.into_inner()).collect();
    let id_ptr: Vec<u64> = id_ptr.into_iter().take(n).map(|x| x.into_inner()).collect();
    let id_pfx: Vec<u64> = id_pfx.into_iter().take(n).map(|x| x.into_inner()).collect();
    // strict total order on ids by name bytes (distinct ids have distinct names)
    let less = |a: u32, b: u32| -> bool {
        let (pa, pb) = (id_pfx[a as usize], id_pfx[b as usize]);
        if pa != pb {
            return pa < pb;
        }
        unpack(id_ptr[a as usize]) < unpack(id_ptr[b as usize])
    };
    let parent: Vec<AtomicU32> = (0..n as u32).into_par_iter().map(AtomicU32::new).collect();
    let find = |mut x: u32| -> u32 {
        loop {
            let p = parent[x as usize].load(Relaxed);
            if p == x {
                return x;
            }
            let gp = parent[p as usize].load(Relaxed);
            if gp != p {
                // path halving: gp is an ancestor of x, so this store keeps the forest valid
                parent[x as usize].store(gp, Relaxed);
            }
            x = gp;
        }
    };
    idpairs.par_iter().for_each(|v| {
        for &(a, b) in v {
            loop {
                let ra = find(a);
                let rb = find(b);
                if ra == rb {
                    break;
                }
                // hang the larger name under the smaller; retry if the larger stopped being a root
                let (big, small) = if less(ra, rb) { (rb, ra) } else { (ra, rb) };
                if parent[big as usize].compare_exchange(big, small, AcqRel, Relaxed).is_ok() {
                    break;
                }
            }
        }
    });
    let root: Vec<u32> = (0..n as u32).into_par_iter().map(|i| find(i)).collect();
    // holes (id_h == 0) are never linked, so they are their own roots and stay out of the map
    let nonroot = root.par_iter().enumerate().filter(|&(i, &r)| r as usize != i).count();
    // parallel fill of the link map
    let cap = (nonroot * 2).next_power_of_two().max(16);
    let lmask = cap - 1;
    let slots: Vec<LinkSlot> = zeroed_vec(cap);
    let base = slots.as_ptr() as usize;
    (0..n).into_par_iter().for_each(|i| {
        let r = root[i] as usize;
        if r == i {
            return;
        }
        let h = id_h[i];
        let mut j = h as usize & lmask;
        loop {
            let sp = (base + j * std::mem::size_of::<LinkSlot>()) as *mut LinkSlot;
            let hk = unsafe { &*(std::ptr::addr_of_mut!((*sp).h) as *const AtomicU64) };
            if hk.compare_exchange(0, h, AcqRel, Acquire).is_ok() {
                unsafe {
                    (*sp).name_h = pkey(unpack(id_ptr[r]));
                    (*sp).name_ptr = id_ptr[r];
                }
                return;
            }
            j = (j + 1) & lmask;
        }
    });
    LinkMap { slots, mask: lmask, len: nonroot }
}

fn build_links(files: &[PathBuf]) -> LinkMap {
    let datas: Vec<Bytes> = files.iter().map(|p| leak_mmap(p)).collect();
    let chunks: Vec<Bytes> = datas
        .into_iter()
        .filter_map(|d| memchr::memchr(b'\n', d).map(|i| &d[i + 1..]))
        .collect();
    // parse on a few scoped threads (the rayon pool is busy with events)
    type Pair = (u64, u64, u64, u64, u64, u64);
    let nt = 4usize.min(chunks.len().max(1));
    let pairs: Vec<Vec<Pair>> = std::thread::scope(|sc| {
        let hs: Vec<_> = (0..nt)
            .map(|t| {
                let chunks = &chunks;
                sc.spawn(move || {
                    let mut v: Vec<Pair> = Vec::new();
                    for c in chunks.iter().skip(t).step_by(nt) {
                        for_each_record::<2>(c, |f| {
                            if !f[0].is_empty() && !f[1].is_empty() {
                                v.push((h1(f[0]), pack(f[0]), name_pfx(f[0]), h1(f[1]), pack(f[1]), name_pfx(f[1])));
                            }
                        });
                    }
                    v
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let total: usize = pairs.iter().map(|v| v.len()).sum();
    // intern IDs: open addressing on the hash, slot = id + 1
    let cap = (total * 2 + 16).next_power_of_two();
    let mask = cap - 1;
    let mut slots: Vec<u32> = vec![0; cap];
    let slots_ptr = slots.as_ptr() as usize;
    let mut id_h: Vec<u64> = Vec::with_capacity(total + 16);
    let mut id_ptr: Vec<u64> = Vec::with_capacity(total + 16);
    let mut id_pfx: Vec<u64> = Vec::with_capacity(total + 16);
    let mut parent: Vec<u32> = Vec::with_capacity(total + 16);
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let gp = parent[parent[x as usize] as usize];
            parent[x as usize] = gp;
            x = gp;
        }
        x
    }
    let mut intern = |h: u64, ptr: u64, pfx: u64, id_h: &mut Vec<u64>, id_ptr: &mut Vec<u64>, id_pfx: &mut Vec<u64>, parent: &mut Vec<u32>| -> u32 {
        let mut i = (h >> 20) as usize & mask;
        loop {
            let e = slots[i];
            if e == 0 {
                let id = id_h.len() as u32;
                id_h.push(h);
                id_ptr.push(ptr);
                id_pfx.push(pfx);
                parent.push(id);
                slots[i] = id + 1;
                return id;
            }
            if id_h[(e - 1) as usize] == h {
                return e - 1;
            }
            i = (i + 1) & mask;
        }
    };
    for v in &pairs {
        for (k, &(ha, pa, xa, hb, pb, xb)) in v.iter().enumerate() {
            if k + 8 < v.len() {
                let n = &v[k + 8];
                unsafe {
                    std::arch::x86_64::_mm_prefetch((slots_ptr as *const u32).add((n.0 >> 20) as usize & mask) as *const i8, std::arch::x86_64::_MM_HINT_T0);
                    std::arch::x86_64::_mm_prefetch((slots_ptr as *const u32).add((n.3 >> 20) as usize & mask) as *const i8, std::arch::x86_64::_MM_HINT_T0);
                }
            }
            let ia = intern(ha, pa, xa, &mut id_h, &mut id_ptr, &mut id_pfx, &mut parent);
            let ib = intern(hb, pb, xb, &mut id_h, &mut id_ptr, &mut id_pfx, &mut parent);
            let ra = find(&mut parent, ia);
            let rb = find(&mut parent, ib);
            if ra != rb {
                // the root always holds the byte-wise smallest name of its set
                let (ra_, rb_) = (ra as usize, rb as usize);
                if name_lt_eq(id_pfx[ra_], id_ptr[ra_], id_pfx[rb_], id_ptr[rb_]) {
                    parent[rb_] = ra;
                } else {
                    parent[ra_] = rb;
                }
            }
        }
    }
    let n = id_h.len();
    let mut nonroot = 0usize;
    let mut root = vec![0u32; n];
    for i in 0..n as u32 {
        let r = find(&mut parent, i);
        root[i as usize] = r;
        if r != i {
            nonroot += 1;
        }
    }
    LinkMap::build(
        (0..n).filter(|&i| root[i] as usize != i).map(|i| {
            let r = root[i] as usize;
            LinkSlot { h: id_h[i], name_h: pkey(unpack(id_ptr[r])), name_ptr: id_ptr[r] }
        }),
        nonroot,
    )
}

// ---------------------------------------------------------------- events

/// Event parsed but not yet placed: kept in a per-thread buffer until the link map is ready.
#[derive(Clone, Copy)]
struct LRec {
    eid_lo: u64,
    user: u64,
    uptr: u64,
    eid_part: u32,
    ingest: u32,
    rel: u32,
    pc: u32,
}

/// Dedup record: event hash, ingest second, and where its person record went
/// (chunk << 22 | person partition << 14 | index), or NOLOC for an invalid event.
#[derive(Clone, Copy)]
struct DRec {
    eid_lo: u64,
    ingest: u32,
    loc: u32,
}
const NOLOC: u32 = u32::MAX;

struct EvChunk {
    pev: Vec<Vec<PEv>>,          // by person partition
    long: Vec<Vec<(u64, u64)>>,  // (person key, name pointer) of long-named persons, by person partition
    drec: Vec<Vec<DRec>>,        // by event partition
}

static LINKS: std::sync::OnceLock<LinkMap> = std::sync::OnceLock::new();

/// Resolve persons for a parsed chunk and scatter it: person records by person partition,
/// dedup records by event partition.
fn place_chunk(ci: usize, buf: &[LRec], linkmap: &LinkMap) -> EvChunk {
    let n = buf.len();
    let mut pev: Vec<Vec<PEv>> = (0..P).map(|_| Vec::with_capacity(part_cap(n))).collect();
    let mut drec: Vec<Vec<DRec>> = (0..P).map(|_| Vec::with_capacity(part_cap(n))).collect();
    let mut long: Vec<Vec<(u64, u64)>> = (0..P).map(|_| Vec::new()).collect();
    const AHEAD: usize = 16;
    for x in 0..n {
        if x + AHEAD < n {
            let r2 = &buf[x + AHEAD];
            if r2.rel != INVALID {
                linkmap.prefetch(r2.user);
            }
        }
        let r = &buf[x];
        let mut loc = NOLOC;
        if r.rel != INVALID {
            let (pk, pptr) = match linkmap.resolve(r.user) {
                Some(x) => x,
                None if r.eid_part >> 31 != 0 => ((r.user & !0xF) | LONGK, r.uptr),
                None => (r.uptr, 0),
            };
            let p = partk(pk);
            if pk & 0xF == LONGK {
                long[p].push((pk, pptr));
            }
            let v = &mut pev[p];
            assert!(v.len() < (1 << 14));
            loc = (ci as u32) << 22 | (p as u32) << 14 | v.len() as u32;
            v.push(PEv { pk, rel: r.rel, pc: r.pc });
        }
        drec[(r.eid_part & 0xFF) as usize].push(DRec { eid_lo: r.eid_lo, ingest: r.ingest, loc });
    }
    EvChunk { pev, long, drec }
}

fn parse_events(files: &[PathBuf], ctx: &Ctx) -> Vec<EvChunk> {
    let total: usize = files.iter().map(|f| fs::metadata(f).map(|m| m.len() as usize).unwrap_or(0)).sum();
    let cmb: usize = std::env::var("SS_CHUNKMB").ok().and_then(|v| v.parse().ok()).unwrap_or(8);
    // Each file adds at most one partial chunk, so keep chunks < 1024 (10 bits in a record location).
    let budget = 1023usize.saturating_sub(files.len()).max(64);
    let chunks = chunks_of(files, (cmb << 20).max(total / budget + 1));
    assert!(chunks.len() < 1024);
    thread_local! {
        static BUF: std::cell::RefCell<Vec<LRec>> = const { std::cell::RefCell::new(Vec::new()) };
    }
    enum Out {
        Done(EvChunk),
        Later(Vec<LRec>),
    }
    let outs: Vec<Out> = chunks
        .par_iter()
        .enumerate()
        .map(|(ci, c)| {
            populate(c);
            BUF.with(|b| {
                let mut buf = b.borrow_mut();
                buf.clear();
                buf.reserve(c.len() / 40);
                for_each_record::<7>(c, |f| {
                let eid = f[0];
                if eid.is_empty() {
                    return;
                }
                let ingest_s = parse_ingest_secs(f[3]);
                let ingest = match ingest_s {
                    Some(t) => t * 1000,
                    None => i64::MAX,
                };
                let mut valid = true;
                let user = f[1];
                if user.is_empty() || f[5].is_empty() {
                    valid = false;
                }
                let ts = match parse_ts(f[2]) {
                    Some(t) if t >= ctx.ws && t < ctx.we => t,
                    _ => {
                        valid = false;
                        0
                    }
                };
                let mut price = 0u32;
                let mut cat = 0u16;
                let et = f[4];
                let is_vc = et.len() == 4 && {
                    let w = u32::from_le_bytes(et[..4].try_into().unwrap());
                    w == u32::from_le_bytes(*b"view") || w == u32::from_le_bytes(*b"cart")
                };
                let is_p = et.len() == 8 && u64::from_le_bytes(et[..8].try_into().unwrap()) == u64::from_le_bytes(*b"purchase");
                match (is_vc, is_p) {
                    (true, _) => {}
                    (_, true) => match parse_price(f[6]) {
                        Some(p) => {
                            price = p;
                            cat = ctx.products.get(f[5]).unwrap_or(ctx.unknown_cat);
                        }
                        None => valid = false,
                    },
                    _ => valid = false,
                }
                if valid && ingest - ts > DAY {
                    valid = false;
                }
                let eh = h2(eid);
                // uptr holds the user's own person key when it is short, else a pointer to it
                // (flagged in bit 31 of eid_part): the key is computed while the line is hot
                let (uh, uptr, rel, lf) = if valid {
                    let k = pkey(user);
                    if k & 0xF == LONGK {
                        (h1(user), pack(user), (ts - ctx.base) as u32, 1u32 << 31)
                    } else {
                        (h1(user), k, (ts - ctx.base) as u32, 0)
                    }
                } else {
                    (0, 0, INVALID, 0)
                };
                let ingest = match ingest_s { Some(t) => t.clamp(0, u32::MAX as i64 - 1) as u32, None => u32::MAX };
                buf.push(LRec { eid_lo: eh.lo, user: uh, uptr, eid_part: part2(eh) as u32 | lf, ingest, rel, pc: (price << 8) | cat as u32 });
                });
                match LINKS.get() {
                    Some(lm) => Out::Done(place_chunk(ci, &buf, lm)),
                    None => Out::Later(std::mem::take(&mut *buf)),
                }
            })
        })
        .collect();
    let nlater = outs.iter().filter(|o| matches!(o, Out::Later(_))).count();
    let lm = {
        while LINKS.get().is_none() {
            std::thread::yield_now();
        }
        LINKS.get().unwrap()
    };
    let res: Vec<EvChunk> = outs
        .into_par_iter()
        .enumerate()
        .map(|(ci, o)| match o {
            Out::Done(e) => e,
            Out::Later(buf) => place_chunk(ci, &buf, lm),
        })
        .collect();
    eprintln!("  ({} of {} chunks placed after links)", nlater, res.len());
    res
}

thread_local! {
    static DEDUP_TABLE: std::cell::RefCell<Vec<u64>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Dedup one event partition; returns the person-record locations of the copies that lost.
fn dedup_partition(q: usize, ev: &[EvChunk]) -> Vec<u32> {
    let n: usize = ev.iter().map(|c| c.drec[q].len()).sum();
    assert!(n < (1 << 24));
    let mut offs: Vec<usize> = Vec::with_capacity(ev.len() + 1);
    offs.push(0);
    for c in ev {
        offs.push(offs.last().unwrap() + c.drec[q].len());
    }
    let get = |j: usize| -> &DRec {
        let ci = offs.partition_point(|&o| o <= j) - 1;
        &ev[ci].drec[q][j - offs[ci]]
    };
    let mut lost: Vec<u32> = Vec::new();
    DEDUP_TABLE.with(|t| {
        let mut t = t.borrow_mut();
        let size = (n * 2).next_power_of_two().max(1024);
        let mask = size - 1;
        t.clear();
        t.resize(size, 0);
        let t = &mut t[..];
        let mut i = 0usize;
        const AHEAD: usize = 8;
        for c in ev {
            let c = &c.drec[q];
            let m = c.len();
            for x in 0..m {
                if x + AHEAD < m {
                    let s2 = c[x + AHEAD].eid_lo as usize & mask;
                    unsafe { std::arch::x86_64::_mm_prefetch(t.as_ptr().add(s2) as *const i8, std::arch::x86_64::_MM_HINT_T0) };
                }
                let r = &c[x];
                let h = r.eid_lo;
                let tag = h >> 24;
                let entry = (tag << 24) | (i as u64 + 1);
                let mut slot = h as usize & mask;
                loop {
                    let e = unsafe { *t.get_unchecked(slot) };
                    if e == 0 {
                        unsafe { *t.get_unchecked_mut(slot) = entry };
                        break;
                    }
                    if e >> 24 == tag {
                        let j = (e & 0xFF_FFFF) as usize - 1;
                        let rj = get(j);
                        if rj.eid_lo == h {
                            // copies with equal ingest are identical: keep the first
                            if r.ingest < rj.ingest {
                                if rj.loc != NOLOC {
                                    lost.push(rj.loc);
                                }
                                unsafe { *t.get_unchecked_mut(slot) = entry };
                            } else if r.loc != NOLOC {
                                lost.push(r.loc);
                            }
                            break;
                        }
                    }
                    slot = (slot + 1) & mask;
                }
                i += 1;
            }
        }
    });
    lost
}

// ---------------------------------------------------------------- clicks

fn parse_clicks(files: &[PathBuf], ctx: &Ctx, linkmap: &LinkMap) -> Vec<Vec<Vec<PCl>>> {
    let chunks = chunks_of(files, 4 << 20);
    chunks
        .par_iter()
        .map(|c| {
            populate(c);
            let mut parts: Vec<Vec<PCl>> = (0..P).map(|_| Vec::with_capacity(part_cap(c.len() / 46))).collect();
            const B: usize = 32;
            let mut batch: Vec<(PCl, u64)> = Vec::with_capacity(B);
            let flush = |batch: &mut Vec<(PCl, u64)>, parts: &mut Vec<Vec<PCl>>| {
                for x in batch.iter() {
                    linkmap.prefetch(x.0.ph);
                }
                for (x, own) in batch.iter() {
                    let pk = match linkmap.resolve(x.ph) {
                        Some((k, _)) => k,
                        None => *own,
                    };
                    parts[partk(pk)].push(PCl { ph: pk, ..*x });
                }
                batch.clear();
            };
            for_each_record::<4>(c, |f| {
                let (cid, user, camp, ts) = (f[0], f[1], f[2], f[3]);
                if cid.is_empty() || user.is_empty() {
                    return;
                }
                let ts = match parse_ts(ts) {
                    Some(t) if t >= ctx.base && t < ctx.we => t,
                    _ => return,
                };
                let camp = match ctx.campaigns.get(camp) {
                    Some(c) => c,
                    None => return,
                };
                batch.push((PCl { ph: h1(user), cid: pack(cid), rel: (ts - ctx.base) as u32, camp }, pkey(user)));
                if batch.len() == B {
                    flush(&mut batch, &mut parts);
                }
            });
            flush(&mut batch, &mut parts);
            parts
        })
        .collect()
}

// ---------------------------------------------------------------- sessions

#[derive(Default)]
struct UserRows {
    user: Vec<Bytes>,
    day: Vec<i32>,
    sessions: Vec<i64>,
    events: Vec<i64>,
    revenue: Vec<i64>,
    top: Vec<Option<u16>>,
}

#[allow(clippy::too_many_arguments)]
fn process_person_partition(
    p: usize,
    ev_person: &[Vec<Vec<PEv>>],
    ev_long: &[Vec<Vec<(u64, u64)>>],
    lost: &[u32],
    click_parts: &[Vec<Vec<PCl>>],
    ws_day: i64,
    base: i64,
    ndays: usize,
    organic: usize,
    cat_rank: &[u16],
    cacc: &mut [u64],
    rows: &mut UserRows,
    stamp: &mut [u32],
    stamp_ctr: &mut u32,
) {
    // intern persons by 128-bit hash
    let mut table: HashTable<u32> = HashTable::new();
    let mut names_h: Vec<u64> = Vec::new();
    // flat index of each chunk's slice, and the losers of dedup as a bitset over it
    let mut offs: Vec<u32> = Vec::with_capacity(ev_person.len() + 1);
    offs.push(0);
    for v in ev_person {
        offs.push(offs.last().unwrap() + v[p].len() as u32);
    }
    let ntot = *offs.last().unwrap() as usize;
    let mut lose = vec![0u64; ntot / 64 + 1];
    for &loc in lost {
        let f = offs[(loc >> 22) as usize] as usize + (loc & 0x3FFF) as usize;
        lose[f / 64] |= 1 << (f % 64);
    }
    let ne = ntot - lost.len();
    let mut epid: Vec<u32> = Vec::with_capacity(ntot);
    let mut ecnt: Vec<u32> = Vec::new();
    let mut fidx = 0usize;
    for v in ev_person {
        for e in &v[p] {
            let f = fidx;
            fidx += 1;
            if lose[f / 64] >> (f % 64) & 1 != 0 {
                epid.push(u32::MAX);
                continue;
            }
            let pid = match table.entry(hashk(e.pk), |&j| names_h[j as usize] == e.pk, |&j| hashk(names_h[j as usize])) {
                hashbrown::hash_table::Entry::Occupied(o) => *o.get(),
                hashbrown::hash_table::Entry::Vacant(v) => {
                    let id = names_h.len() as u32;
                    names_h.push(e.pk);
                    ecnt.push(0);
                    v.insert(id);
                    id
                }
            };
            ecnt[pid as usize] += 1;
            epid.push(pid);
        }
    }
    let np = names_h.len();
    // names: short keys decode in place; long ones come from the chunks' side lists
    let names: Vec<Bytes> = {
        let mut longs: HashMap<u64, u64> = HashMap::new();
        for v in ev_long {
            for &(k, ptr) in &v[p] {
                longs.insert(k, ptr);
            }
        }
        let mut arena: Vec<u8> = Vec::with_capacity(np * 10);
        let mut spans: Vec<(u32, u32)> = Vec::with_capacity(np);
        for &k in &names_h {
            if k & 0xF == LONGK {
                spans.push((u32::MAX, 0));
            } else {
                let a = arena.len();
                decode_key(k, &mut arena);
                spans.push((a as u32, (arena.len() - a) as u32));
            }
        }
        let arena: &'static [u8] = Box::leak(arena.into_boxed_slice());
        names_h
            .iter()
            .zip(spans.iter())
            .map(|(&k, &(a, l))| if a == u32::MAX { unpack(longs[&k]) } else { &arena[a as usize..(a + l) as usize] })
            .collect()
    };
    // counting sort: evs grouped by person as (rel << 32 | pc)
    let mut eoff = vec![0u32; np + 1];
    for i in 0..np {
        eoff[i + 1] = eoff[i] + ecnt[i];
    }
    let mut evs: Vec<u64> = Vec::with_capacity(ne);
    {
        let mut pos = eoff.clone();
        let dst = evs.spare_capacity_mut();
        let mut x = 0usize;
        for v in ev_person {
            for e in &v[p] {
                let pid = epid[x] as usize;
                x += 1;
                if pid == u32::MAX as usize {
                    continue;
                }
                dst[pos[pid] as usize].write((e.rel as u64) << 32 | e.pc as u64);
                pos[pid] += 1;
            }
        }
        unsafe { evs.set_len(ne) };
    }
    drop(epid);
    for w in eoff.windows(2) {
        let sl = &mut evs[w[0] as usize..w[1] as usize];
        if sl.len() > 1 {
            sl.sort_unstable();
        }
    }
    let nc: usize = click_parts.iter().map(|v| v[p].len()).sum();
    // clicks of persons with events, grouped by person: key = ts << 32 | campaign
    let mut cpid: Vec<u32> = Vec::with_capacity(nc);
    let mut cls0: Vec<Ck> = Vec::with_capacity(nc);
    for v in click_parts {
        for c in &v[p] {
            if let Some(&pid) = table.find(hashk(c.ph), |&j| names_h[j as usize] == c.ph) {
                cpid.push(pid);
                cls0.push(Ck { k: (c.rel as u64) << 32 | c.camp as u64, cid: c.cid });
            }
        }
    }
    let mut coff = vec![0u32; np + 1];
    for &x in &cpid {
        coff[x as usize + 1] += 1;
    }
    for i in 0..np {
        coff[i + 1] += coff[i];
    }
    let mut cls: Vec<Ck> = Vec::with_capacity(cls0.len());
    {
        let mut pos = coff.clone();
        let dst = cls.spare_capacity_mut();
        for (x, c) in cpid.iter().zip(cls0.iter()) {
            dst[pos[*x as usize] as usize].write(*c);
            pos[*x as usize] += 1;
        }
        unsafe { cls.set_len(cls0.len()) };
    }
    drop(cls0);
    drop(cpid);
    for w in coff.windows(2) {
        let sl = &mut cls[w[0] as usize..w[1] as usize];
        if sl.len() > 1 {
            sl.sort_unstable_by_key(|c| c.k);
        }
    }

    let ncat = cat_rank.len();
    let mut catsum = vec![0i64; ncat];
    let mut starts: Vec<i64> = Vec::new();
    let mut sdays: Vec<usize> = Vec::new();
    for pid in 0..np {
        let i = eoff[pid] as usize;
        let j = eoff[pid + 1] as usize;
        let pcl = &cls[coff[pid] as usize..coff[pid + 1] as usize];
        let name = names[pid as usize];
        // Persons with many clicks (shared devices) use an interval sweep for assists.
        let heavy = pcl.len() > heavy_threshold();
        starts.clear();
        sdays.clear();

        // user-day accumulator
        let mut cur_day: i64 = i64::MIN;
        let (mut d_sess, mut d_ev, mut d_rev) = (0i64, 0i64, 0i64);
        let mut any_purchase = false;
        let mut lo = 0usize;
        let mut hi = 0usize;

        let mut k = i;
        while k < j {
            // session [k, m)
            let start = (evs[k] >> 32) as i64;
            let day = (start + base).div_euclid(DAY);
            if day != cur_day {
                if cur_day != i64::MIN {
                    flush_day(rows, name, cur_day, d_sess, d_ev, d_rev, any_purchase, &mut catsum, cat_rank);
                }
                cur_day = day;
                d_sess = 0;
                d_ev = 0;
                d_rev = 0;
                any_purchase = false;
            }
            let mut prev = start;
            let mut m = k;
            let (mut s_ev, mut s_rev, mut s_purch) = (0i64, 0i64, false);
            while m < j {
                let t = (evs[m] >> 32) as i64;
                if t - prev > GAP {
                    break;
                }
                prev = t;
                s_ev += 1;
                let pc = evs[m] as u32;
                let pr = pc >> 8;
                if pr > 0 {
                    s_rev += pr as i64;
                    s_purch = true;
                    catsum[(pc & 0xFF) as usize] += pr as i64;
                }
                m += 1;
            }
            // day (abs ms = rel + base). base = ws - 7d, ws on a midnight usually but compute generally
            d_sess += 1;
            d_ev += s_ev;
            d_rev += s_rev;
            any_purchase |= s_purch;

            // attribution
            while hi < pcl.len() && ((pcl[hi].k >> 32) as i64) <= start {
                hi += 1;
            }
            while lo < hi && ((pcl[lo].k >> 32) as i64) < start - WEEK {
                lo += 1;
            }
            let didx = (day - ws_day) as usize;
            let credited = if hi > lo {
                // latest click; on a tie in ts, the smallest click_id
                let mut q = hi - 1;
                let t = pcl[q].k >> 32;
                let mut best = q;
                while q > lo && pcl[q - 1].k >> 32 == t {
                    q -= 1;
                    if unpack(pcl[q].cid) < unpack(pcl[best].cid) {
                        best = q;
                    }
                }
                (pcl[best].k & 0xFFFF) as usize
            } else {
                organic
            };
            let base_i = (credited * ndays + didx) * 4;
            cacc[base_i] += 1;
            if s_purch {
                cacc[base_i + 1] += 1;
            }
            cacc[base_i + 2] += s_rev as u64;
            if heavy {
                starts.push(start);
                sdays.push(didx);
            } else if hi > lo {
                *stamp_ctr = stamp_ctr.wrapping_add(1);
                if *stamp_ctr == u32::MAX {
                    stamp.iter_mut().for_each(|x| *x = u32::MAX - 1);
                    *stamp_ctr = 0;
                }
                let sc = *stamp_ctr;
                for c in &pcl[lo..hi] {
                    let cc = (c.k & 0xFFFF) as usize;
                    if stamp[cc] != sc {
                        stamp[cc] = sc;
                        cacc[(cc * ndays + didx) * 4 + 3] += 1;
                    }
                }
            }
            k = m;
        }
        if cur_day != i64::MIN {
            flush_day(rows, name, cur_day, d_sess, d_ev, d_rev, any_purchase, &mut catsum, cat_rank);
        }
        if heavy {
            heavy_assists(pcl, &starts, &sdays, ndays, cacc);
        }
    }
}

fn heavy_threshold() -> usize {
    static T: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *T.get_or_init(|| std::env::var("SS_HEAVY").ok().and_then(|s| s.parse().ok()).unwrap_or(64))
}

/// Assists for one person via merged click intervals: a campaign assists a session starting
/// at s iff one of its clicks c has c <= s <= c + 7d. Sessions are sorted by start.
fn heavy_assists(pcl: &[Ck], starts: &[i64], sdays: &[usize], ndays: usize, cacc: &mut [u64]) {
    if starts.is_empty() {
        return;
    }
    let mut day_first = vec![0usize; ndays + 1];
    {
        let mut k = 0usize;
        for d in 0..=ndays {
            while k < sdays.len() && sdays[k] < d {
                k += 1;
            }
            day_first[d] = k;
        }
    }
    let mut cc: Vec<(u16, i64)> = pcl.iter().map(|c| ((c.k & 0xFFFF) as u16, (c.k >> 32) as i64)).collect();
    cc.sort_unstable();
    let mut x = 0usize;
    while x < cc.len() {
        let camp = cc[x].0 as usize;
        let mut a = cc[x].1;
        let mut b = a + WEEK;
        x += 1;
        loop {
            let more = x < cc.len() && cc[x].0 as usize == camp;
            if more && cc[x].1 <= b {
                b = b.max(cc[x].1 + WEEK);
                x += 1;
                continue;
            }
            // flush [a, b]
            let i0 = starts.partition_point(|&s| s < a);
            let i1 = starts.partition_point(|&s| s <= b);
            if i0 < i1 {
                for d in sdays[i0]..=sdays[i1 - 1] {
                    let lo = i0.max(day_first[d]);
                    let hi = i1.min(day_first[d + 1]);
                    if hi > lo {
                        cacc[(camp * ndays + d) * 4 + 3] += (hi - lo) as u64;
                    }
                }
            }
            if !more {
                break;
            }
            a = cc[x].1;
            b = a + WEEK;
            x += 1;
        }
    }
}


#[allow(clippy::too_many_arguments)]
#[inline]
fn flush_day(
    rows: &mut UserRows,
    name: Bytes,
    day: i64,
    s: i64,
    e: i64,
    r: i64,
    any_purchase: bool,
    catsum: &mut [i64],
    cat_rank: &[u16],
) {
    let mut top: Option<u16> = None;
    if any_purchase {
        let mut best: i64 = -1;
        for c in 0..catsum.len() {
            let v = catsum[c];
            if v > 0 {
                let better = match top {
                    None => true,
                    Some(t) => v > best || (v == best && cat_rank[c] < cat_rank[t as usize]),
                };
                if better {
                    best = v;
                    top = Some(c as u16);
                }
            }
            catsum[c] = 0;
        }
    }
    rows.user.push(name);
    rows.day.push(day as i32);
    rows.sessions.push(s);
    rows.events.push(e);
    rows.revenue.push(r);
    rows.top.push(top);
}

// ---------------------------------------------------------------- output

fn props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
        .build()
}

fn write_user_rows(path: &Path, rows: &UserRows, cat_names: &[String]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]));
    let users = StringArray::from_iter_values(rows.user.iter().map(|b| unsafe { std::str::from_utf8_unchecked(b) }));
    let tops: StringArray = rows.top.iter().map(|t| t.map(|c| cat_names[c as usize].as_str())).collect();
    let cols: Vec<ArrayRef> = vec![
        Arc::new(users),
        Arc::new(Date32Array::from(rows.day.clone())),
        Arc::new(Int64Array::from(rows.sessions.clone())),
        Arc::new(Int64Array::from(rows.events.clone())),
        Arc::new(Int64Array::from(rows.revenue.clone())),
        Arc::new(tops),
    ];
    let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
    let f = fs::File::create(path).unwrap();
    let mut w = ArrowWriter::try_new(std::io::BufWriter::with_capacity(1 << 20, f), schema, Some(props())).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn write_campaign(
    path: &Path,
    acc: &[u64],
    ncamp: usize,
    ndays: usize,
    ws_day: i64,
    camp_ids: &[Bytes],
    camp_chan: &[Bytes],
) {
    let mut cid: Vec<&str> = Vec::new();
    let mut ch: Vec<&str> = Vec::new();
    let mut day: Vec<i32> = Vec::new();
    let (mut s, mut ps, mut rv, mut asst) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for c in 0..=ncamp {
        for d in 0..ndays {
            let b = (c * ndays + d) * 4;
            if acc[b] == 0 && acc[b + 3] == 0 {
                continue;
            }
            if c == ncamp {
                cid.push("organic");
                ch.push("organic");
            } else {
                cid.push(unsafe { std::str::from_utf8_unchecked(camp_ids[c]) });
                ch.push(unsafe { std::str::from_utf8_unchecked(camp_chan[c]) });
            }
            day.push((ws_day + d as i64) as i32);
            s.push(acc[b] as i64);
            ps.push(acc[b + 1] as i64);
            rv.push(acc[b + 2] as i64);
            asst.push(acc[b + 3] as i64);
        }
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]));
    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(cid)),
        Arc::new(StringArray::from(ch)),
        Arc::new(Date32Array::from(day)),
        Arc::new(Int64Array::from(s)),
        Arc::new(Int64Array::from(ps)),
        Arc::new(Int64Array::from(rv)),
        Arc::new(Int64Array::from(asst)),
    ];
    let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
    let f = fs::File::create(path).unwrap();
    let mut w = ArrowWriter::try_new(std::io::BufWriter::new(f), schema, Some(props())).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

#[cfg(test)]
mod ts_tests {
    use super::*;
    fn reference(s: &[u8]) -> Option<i64> {
        // straightforward F1/F2/F3 parser
        let n = s.len();
        if n == 20 || n == 25 {
            if s[10] != b'T' { return parse_f3(s); }
            let num = |a: usize, b: usize| -> Option<i64> {
                if s[a..b].iter().all(|c| c.is_ascii_digit()) { std::str::from_utf8(&s[a..b]).ok()?.parse().ok() } else { None }
            };
            if s[4] != b'-' || s[7] != b'-' || s[13] != b':' || s[16] != b':' { return None; }
            let (y, mo, d, h, mi, se) = (num(0,4)?, num(5,7)?, num(8,10)?, num(11,13)?, num(14,16)?, num(17,19)?);
            if !(1..=12).contains(&mo) || d < 1 || h > 23 || mi > 59 || se > 59 { return None; }
            let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
            let dim = [31, if leap {29} else {28}, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][mo as usize - 1];
            if d > dim { return None; }
            let t = ((days_from_civil(y, mo as u32, d as u32) * 24 + h) * 60 + mi) * 60_000 + se * 1000;
            if n == 20 { return if s[19] == b'Z' { Some(t) } else { None }; }
            if (s[19] != b'+' && s[19] != b'-') || s[22] != b':' { return None; }
            let (oh, om) = (num(20,22)?, num(23,25)?);
            if oh > 23 || om > 59 { return None; }
            let off = (oh * 60 + om) * 60_000;
            return Some(if s[19] == b'+' { t - off } else { t + off });
        }
        parse_f3(s)
    }
    fn parse_f3(s: &[u8]) -> Option<i64> {
        if s.is_empty() || !s.iter().all(|c| c.is_ascii_digit()) { return None; }
        let v: u128 = std::str::from_utf8(s).unwrap().parse().unwrap_or(u128::MAX);
        Some(if v > i64::MAX as u128 { i64::MAX } else { v as i64 })
    }
    #[test]
    fn dates_exhaustive() {
        for y in [1899, 1900, 1970, 1999, 2000, 2024, 2026, 2027, 2028, 2100, 2199, 2200, 9999, 0] {
            for mo in 0..=13 { for d in 0..=32 {
                for tail in ["Z", "+05:30", "-23:59", "+24:00", "Q"] {
                    let s = format!("{:04}-{:02}-{:02}T12:34:56{}", y, mo, d, tail);
                    assert_eq!(parse_ts(s.as_bytes()), reference(s.as_bytes()), "{}", s);
                    if tail == "Z" && reference(s.as_bytes()).is_some() {
                        assert_eq!(parse_ingest_secs(s.as_bytes()), reference(s.as_bytes()).map(|t| t / 1000), "{}", s);
                    }
                }
            }}
        }
    }
    #[test]
    fn f3_random() {
        let mut x = 0x1234_5678_9abc_def0u64;
        let alpha = b"0123456789012345678901234567890123456789-:Tx ";
        for i in 0..2_000_000u64 {
            x ^= x << 13; x ^= x >> 7; x ^= x << 17;
            let n = [13, 13, 13, 12, 14, 1, 19, 20, 21][(i % 9) as usize];
            let mut s = Vec::new();
            let mut y = x;
            for _ in 0..n { y = y.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); s.push(alpha[((y >> 33) % if i % 3 == 0 { alpha.len() as u64 } else { 10 }) as usize]); }
            assert_eq!(parse_ts(&s), reference(&s), "{:?}", std::str::from_utf8(&s));
        }
    }
}
