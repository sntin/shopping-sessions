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
#[inline(always)]
fn for_each_record<const N: usize>(chunk: Bytes, mut f: impl FnMut(&[Bytes; N])) {
    let n = chunk.len();
    let mut fields: [Bytes; N] = [&[]; N];
    let mut k = 0usize; // fields completed in current line
    let mut bad = false;
    let mut start = 0usize; // current field start
    let mut qcount = 0u32;
    let mut base = 0usize;
    let mut tail = [0u8; 64];
    // finish field ending at `pos` (exclusive)
    macro_rules! end_field {
        ($pos:expr) => {{
            let pos = $pos;
            if k < N {
                let mut fl = &chunk[start..pos];
                if qcount != 0 {
                    let l = fl.len();
                    if qcount != 2 || l < 2 || fl[0] != b'"' || fl[l - 1] != b'"' {
                        bad = true;
                    } else {
                        fl = &fl[1..l - 1];
                    }
                }
                fields[k] = fl;
            } else {
                bad = true;
            }
            k += 1;
            qcount = 0;
            start = pos + 1;
        }};
    }
    while base < n {
        let (sep, mut qm, nlm) = if base + 64 <= n {
            block_masks(unsafe { &*(chunk.as_ptr().add(base) as *const [u8; 64]) })
        } else {
            tail = [0u8; 64];
            tail[..n - base].copy_from_slice(&chunk[base..]);
            block_masks(&tail)
        };
        let mut m = sep;
        while m != 0 {
            let b = m.trailing_zeros();
            let below = if b == 0 { 0 } else { qm & (u64::MAX >> (64 - b)) };
            qcount += below.count_ones();
            qm &= !below;
            let pos = base + b as usize;
            end_field!(pos);
            if (nlm >> b) & 1 != 0 {
                if !bad && k == N {
                    f(&fields);
                }
                k = 0;
                bad = false;
            }
            m &= m - 1;
        }
        qcount += qm.count_ones();
        base += 64;
    }
    // last line without a trailing newline
    if start < n {
        end_field!(n);
        if !bad && k == N {
            f(&fields);
        }
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
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let m = m as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[inline(always)]
fn is_leap(y: u32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

#[inline(always)]
fn parse_datetime(s: &[u8]) -> Option<i64> {
    // YYYY-MM-DDTHH:MM:SS (first 19 bytes), caller checked len and s[10]=='T'
    if s[4] != b'-' || s[7] != b'-' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = d2(s, 0)? * 100 + d2(s, 2)?;
    let mo = d2(s, 5)?;
    let d = d2(s, 8)?;
    let h = d2(s, 11)?;
    let mi = d2(s, 14)?;
    let se = d2(s, 17)?;
    if mo < 1 || mo > 12 || d < 1 || h > 23 || mi > 59 || se > 59 {
        return None;
    }
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
    let days = days_from_civil(y as i64, mo, d);
    Some(((days * 24 + h as i64) * 60 + mi as i64) * 60_000 + se as i64 * 1000)
}

/// Parse an event/click ts to epoch ms. None = bad timestamp.
#[inline(always)]
fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 20 && s[10] == b'T' {
        if s[19] != b'Z' {
            return None;
        }
        return parse_datetime(s);
    }
    if n == 25 && s[10] == b'T' {
        let sign = s[19];
        if (sign != b'+' && sign != b'-') || s[22] != b':' {
            return None;
        }
        let oh = d2(s, 20)?;
        let om = d2(s, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let t = parse_datetime(s)?;
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
    let m = unsafe { memmap2::MmapOptions::new().populate().map(&f) }.expect("mmap");
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

#[inline(always)]
fn part2(h: H2) -> usize {
    (h.hi >> 40) as usize & (P - 1)
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

#[derive(Clone, Copy)]
struct EvRec {
    eid_lo: u64,
    user: H2,
    uptr: u64,
    eid_hi: u32,
    ingest: u32, // seconds since epoch (ingest_ts is always whole-second F1)
    rel: u32,    // ts - (ws - 7d); u32::MAX = invalid event
    pc: u32,     // price << 8 | category; price 0 = not a purchase
}
const INVALID: u32 = u32::MAX;

#[derive(Clone, Copy)]
struct PEv {
    ph: H2,
    pptr: u64,
    rel: u32, // ts - (ws - 7d)
    pc: u32,
}

#[derive(Clone, Copy)]
struct PCl {
    ph: H2,
    cid: u64,
    rel: u32,
    camp: u16,
}

struct Ctx {
    ws: i64,
    we: i64,
    base: i64, // ws - 7d
    products: HashMap<Bytes, u16>,
    unknown_cat: u16,
    campaigns: HashMap<Bytes, u16>,
}

fn main() {
    let t0 = Instant::now();
    let data_dir = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out_dir = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let ncpu: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8));
    rayon::ThreadPoolBuilder::new().num_threads(ncpu.max(1)).build_global().unwrap();

    let manifest = fs::read_to_string(data_dir.join("manifest.json")).expect("manifest");
    let ws = parse_ts(json_str(&manifest, "window_start").as_bytes()).expect("window_start");
    let we = parse_ts(json_str(&manifest, "window_end").as_bytes()).expect("window_end");
    let base = ws - WEEK;

    // products / categories
    let mut cat_names: Vec<Bytes> = Vec::new();
    let mut cat_idx: HashMap<Bytes, u16> = HashMap::new();
    let mut products: HashMap<Bytes, u16> = HashMap::new();
    for (pid, cat) in read_small_table(&data_dir.join("products.csv")) {
        let c = *cat_idx.entry(cat).or_insert_with(|| {
            cat_names.push(cat);
            (cat_names.len() - 1) as u16
        });
        products.entry(pid).or_insert(c);
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
    let mut campaigns: HashMap<Bytes, u16> = HashMap::new();
    for (cid, ch) in read_small_table(&data_dir.join("campaigns.csv")) {
        if !campaigns.contains_key(cid) {
            campaigns.insert(cid, camp_ids.len() as u16);
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
    let link_thread = std::thread::spawn(move || {
        let lm = build_links(&ln_files);
        eprintln!("[{:?}] links done ({} remapped ids)", t0.elapsed(), lm.len);
        lm
    });
    let ev_kept = parse_events(&ev_files, &ctx);
    eprintln!("[{:?}] events parsed", t0.elapsed());
    let linkmap = link_thread.join().unwrap();
    let click_parts = parse_clicks(&cl_files, &ctx, &linkmap);
    eprintln!("[{:?}] clicks parsed", t0.elapsed());

    // ---- stage 2: dedup each eid partition, resolve person, repartition by person
    // transpose chunk-major -> partition-major so each partition's input is freed after use
    let mut by_part: Vec<Vec<Vec<EvRec>>> = (0..P).map(|_| Vec::with_capacity(ev_kept.len())).collect();
    for chunk in ev_kept {
        for (p, v) in chunk.into_iter().enumerate() {
            by_part[p].push(v);
        }
    }
    let ev_person: Vec<Vec<Vec<PEv>>> = by_part
        .into_par_iter()
        .map(|chunks| {
            let mut out: Vec<Vec<PEv>> = (0..P).map(|_| Vec::new()).collect();
            dedup_partition(chunks, &linkmap, &mut out);
            out
        })
        .collect();
    eprintln!("[{:?}] dedup done", t0.elapsed());

    // ---- stage 3: per person partition: sessionize, attribute, aggregate
    let ndays = ((we - 1).div_euclid(DAY) - ws.div_euclid(DAY) + 1).max(1) as usize;
    let ws_day = ws.div_euclid(DAY);
    let user_dir = out_dir.join("user_daily");
    let camp_dir = out_dir.join("campaign_daily");
    fs::create_dir_all(&user_dir).unwrap();
    fs::create_dir_all(&camp_dir).unwrap();

    let cat_names_s: Vec<String> = cat_names.iter().map(|b| String::from_utf8_lossy(b).into_owned()).collect();

    const GROUP: usize = 8; // partitions per output file
    let camp_acc: Vec<u64> = (0..P / GROUP)
        .into_par_iter()
        .map(|g| {
            let mut cacc = vec![0u64; (ncamp + 1) * ndays * 4];
            let mut rows = UserRows::default();
            let mut stamp = vec![u32::MAX; ncamp + 1];
            let mut stamp_ctr = 0u32;
            for p in g * GROUP..(g + 1) * GROUP {
                process_person_partition(
                    p,
                    &ev_person,
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
            if !rows.day.is_empty() || g == 0 {
                write_user_rows(&user_dir.join(format!("part-{:05}.parquet", g)), &rows, &cat_names_s);
            }
            cacc
        })
        .reduce_with(|mut a, b| {
            for (x, y) in a.iter_mut().zip(b.iter()) {
                *x += *y;
            }
            a
        })
        .unwrap();
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
    h: H2,
    name_h: H2,
    name_ptr: u64,
}

struct LinkMap {
    slots: Vec<LinkSlot>,
    mask: usize,
    len: usize,
}

impl LinkMap {
    #[inline(always)]
    fn prefetch(&self, h: H2) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::*;
            let p = self.slots.as_ptr().add(h.lo as usize & self.mask) as *const i8;
            _mm_prefetch(p, _MM_HINT_T0);
            _mm_prefetch(p.add(40), _MM_HINT_T0);
        }
    }
    /// Resolve an ID to its person (hash, name pointer).
    #[inline(always)]
    fn resolve(&self, h: H2, ptr: u64) -> (H2, u64) {
        let mut i = h.lo as usize & self.mask;
        loop {
            let s = unsafe { self.slots.get_unchecked(i) };
            if s.h == h {
                return (s.name_h, s.name_ptr);
            }
            if s.h.lo == 0 && s.h.hi == 0 {
                return (h, ptr);
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
            let mut i = e.h.lo as usize & mask;
            while !(slots[i].h.lo == 0 && slots[i].h.hi == 0) {
                i = (i + 1) & mask;
            }
            slots[i] = e;
            len += 1;
        }
        LinkMap { slots, mask, len }
    }
}

fn build_links(files: &[PathBuf]) -> LinkMap {
    let datas: Vec<Bytes> = files.iter().map(|p| leak_mmap(p)).collect();
    let chunks: Vec<Bytes> = datas
        .into_iter()
        .filter_map(|d| memchr::memchr(b'\n', d).map(|i| &d[i + 1..]))
        .collect();
    let pairs: Vec<Vec<(H2, u64, H2, u64)>> = chunks
        .iter()
        .map(|c| {
            let mut v = Vec::new();
            for_each_record::<2>(c, |f| {
                if !f[0].is_empty() && !f[1].is_empty() {
                    v.push((h2(f[0]), pack(f[0]), h2(f[1]), pack(f[1])));
                }
            });
            v
        })
        .collect();
    let total: usize = pairs.iter().map(|v| v.len()).sum();
    let mut table: HashTable<u32> = HashTable::with_capacity(total + 16);
    let mut id_h: Vec<H2> = Vec::with_capacity(total + 16);
    let mut id_ptr: Vec<u64> = Vec::with_capacity(total + 16);
    let mut parent: Vec<u32> = Vec::with_capacity(total + 16);
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let gp = parent[parent[x as usize] as usize];
            parent[x as usize] = gp;
            x = gp;
        }
        x
    }
    let mut intern = |h: H2, ptr: u64, id_h: &mut Vec<H2>, id_ptr: &mut Vec<u64>, parent: &mut Vec<u32>| -> u32 {
        match table.entry(h.lo, |&j| id_h[j as usize] == h, |&j| id_h[j as usize].lo) {
            hashbrown::hash_table::Entry::Occupied(o) => *o.get(),
            hashbrown::hash_table::Entry::Vacant(v) => {
                let id = id_h.len() as u32;
                id_h.push(h);
                id_ptr.push(ptr);
                parent.push(id);
                v.insert(id);
                id
            }
        }
    };
    for v in &pairs {
        for &(ha, pa, hb, pb) in v {
            let ia = intern(ha, pa, &mut id_h, &mut id_ptr, &mut parent);
            let ib = intern(hb, pb, &mut id_h, &mut id_ptr, &mut parent);
            let ra = find(&mut parent, ia);
            let rb = find(&mut parent, ib);
            if ra != rb {
                // the root always holds the byte-wise smallest name of its set
                if unpack(id_ptr[ra as usize]) <= unpack(id_ptr[rb as usize]) {
                    parent[rb as usize] = ra;
                } else {
                    parent[ra as usize] = rb;
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
            LinkSlot { h: id_h[i], name_h: id_h[r], name_ptr: id_ptr[r] }
        }),
        nonroot,
    )
}

// ---------------------------------------------------------------- events

fn parse_events(files: &[PathBuf], ctx: &Ctx) -> Vec<Vec<Vec<EvRec>>> {
    let chunks = chunks_of(files, 8 << 20);
    chunks
        .par_iter()
        .map(|c| {
            let mut parts: Vec<Vec<EvRec>> = (0..P).map(|_| Vec::with_capacity(c.len() / 70 / P * 5 / 4 + 4)).collect();
            for_each_record::<7>(c, |f| {
                let eid = f[0];
                if eid.is_empty() {
                    return;
                }
                let ingest = match parse_ts(f[3]) {
                    Some(t) => t,
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
                match f[4] {
                    b"view" | b"cart" => {}
                    b"purchase" => match parse_price(f[6]) {
                        Some(p) => {
                            price = p;
                            cat = *ctx.products.get(f[5]).unwrap_or(&ctx.unknown_cat);
                        }
                        None => valid = false,
                    },
                    _ => valid = false,
                }
                if valid && ingest - ts > DAY {
                    valid = false;
                }
                let eh = h2(eid);
                let (uh, uptr, rel) = if valid { (h2(user), pack(user), (ts - ctx.base) as u32) } else { (H2::default(), 0, INVALID) };
                let ingest = if ingest == i64::MAX { u32::MAX } else { (ingest / 1000).clamp(0, u32::MAX as i64 - 1) as u32 };
                parts[part2(eh)].push(EvRec {
                    eid_lo: eh.lo,
                    user: uh,
                    uptr,
                    eid_hi: eh.hi as u32,
                    ingest,
                    rel,
                    pc: (price << 8) | cat as u32,
                });
            });
            parts
        })
        .collect()
}

fn dedup_partition(chunks: Vec<Vec<EvRec>>, linkmap: &LinkMap, out: &mut [Vec<PEv>]) {
    let n: usize = chunks.iter().map(|c| c.len()).sum();
    let mut recs: Vec<EvRec> = Vec::with_capacity(n);
    for c in chunks {
        recs.extend_from_slice(&c);
    }
    let mut table: HashTable<u32> = HashTable::with_capacity(n);
    let mut keep = vec![true; n];
    for i in 0..n {
        let r = &recs[i];
        match table.entry(
            r.eid_lo,
            |&j| recs[j as usize].eid_lo == r.eid_lo && recs[j as usize].eid_hi == r.eid_hi,
            |&j| recs[j as usize].eid_lo,
        ) {
            hashbrown::hash_table::Entry::Occupied(mut o) => {
                let j = *o.get() as usize;
                if r.ingest < recs[j].ingest {
                    keep[j] = false;
                    *o.get_mut() = i as u32;
                } else {
                    keep[i] = false;
                }
            }
            hashbrown::hash_table::Entry::Vacant(v) => {
                v.insert(i as u32);
            }
        }
    }
    drop(table);
    let cap = n / P * 5 / 4 + 16;
    for o in out.iter_mut() {
        o.reserve(cap);
    }
    const AHEAD: usize = 16;
    for i in 0..n {
        if i + AHEAD < n {
            let r2 = &recs[i + AHEAD];
            if r2.rel != INVALID {
                linkmap.prefetch(r2.user);
            }
        }
        let r = &recs[i];
        if !keep[i] || r.rel == INVALID {
            continue;
        }
        let (ph, pptr) = linkmap.resolve(r.user, r.uptr);
        out[part2(ph)].push(PEv { ph, pptr, rel: r.rel, pc: r.pc });
    }
}

// ---------------------------------------------------------------- clicks

fn parse_clicks(files: &[PathBuf], ctx: &Ctx, linkmap: &LinkMap) -> Vec<Vec<Vec<PCl>>> {
    let chunks = chunks_of(files, 4 << 20);
    chunks
        .par_iter()
        .map(|c| {
            let mut parts: Vec<Vec<PCl>> = (0..P).map(|_| Vec::with_capacity(c.len() / 50 / P * 5 / 4 + 4)).collect();
            const B: usize = 32;
            let mut batch: Vec<PCl> = Vec::with_capacity(B);
            let flush = |batch: &mut Vec<PCl>, parts: &mut Vec<Vec<PCl>>| {
                for x in batch.iter() {
                    linkmap.prefetch(x.ph);
                }
                for x in batch.iter() {
                    let (ph, _) = linkmap.resolve(x.ph, 0);
                    parts[part2(ph)].push(PCl { ph, ..*x });
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
                    Some(&c) => c,
                    None => return,
                };
                batch.push(PCl { ph: h2(user), cid: pack(cid), rel: (ts - ctx.base) as u32, camp });
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
    let mut names_h: Vec<H2> = Vec::new();
    let mut names: Vec<Bytes> = Vec::new();
    let ne: usize = ev_person.iter().map(|v| v[p].len()).sum();
    // (key = pid<<32 | rel, price, cat)
    let mut evs: Vec<(u64, u32)> = Vec::with_capacity(ne);
    for v in ev_person {
        for e in &v[p] {
            let pid = match table.entry(e.ph.lo, |&j| names_h[j as usize] == e.ph, |&j| names_h[j as usize].lo) {
                hashbrown::hash_table::Entry::Occupied(o) => *o.get(),
                hashbrown::hash_table::Entry::Vacant(v) => {
                    let id = names_h.len() as u32;
                    names_h.push(e.ph);
                    names.push(unpack(e.pptr));
                    v.insert(id);
                    id
                }
            };
            evs.push(((pid as u64) << 32 | e.rel as u64, e.pc));
        }
    }
    let nc: usize = click_parts.iter().map(|v| v[p].len()).sum();
    let mut cls: Vec<(u64, Bytes, u16)> = Vec::with_capacity(nc);
    for v in click_parts {
        for c in &v[p] {
            // only persons that have events matter
            if let Some(&pid) = table.find(c.ph.lo, |&j| names_h[j as usize] == c.ph) {
                cls.push(((pid as u64) << 32 | c.rel as u64, unpack(c.cid), c.camp));
            }
        }
    }
    evs.sort_unstable_by_key(|e| e.0);
    cls.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(b.1)));

    let ncat = cat_rank.len();
    let mut catsum = vec![0i64; ncat];
    let mut ci = 0usize; // click cursor (global over sorted cls)
    let mut starts: Vec<i64> = Vec::new();
    let mut sdays: Vec<usize> = Vec::new();
    let mut i = 0usize;
    while i < evs.len() {
        let pid = evs[i].0 >> 32;
        let mut j = i;
        while j < evs.len() && evs[j].0 >> 32 == pid {
            j += 1;
        }
        // clicks of this pid
        while ci < cls.len() && cls[ci].0 >> 32 < pid {
            ci += 1;
        }
        let c0 = ci;
        while ci < cls.len() && cls[ci].0 >> 32 == pid {
            ci += 1;
        }
        let pcl = &cls[c0..ci];
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
            let start = (evs[k].0 & 0xFFFF_FFFF) as i64;
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
                let t = (evs[m].0 & 0xFFFF_FFFF) as i64;
                if t - prev > GAP {
                    break;
                }
                prev = t;
                s_ev += 1;
                let pc = evs[m].1;
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
            while hi < pcl.len() && ((pcl[hi].0 & 0xFFFF_FFFF) as i64) <= start {
                hi += 1;
            }
            while lo < hi && ((pcl[lo].0 & 0xFFFF_FFFF) as i64) < start - WEEK {
                lo += 1;
            }
            let didx = (day - ws_day) as usize;
            let credited = if hi > lo {
                let mut q = hi - 1;
                let t = pcl[q].0;
                while q > lo && pcl[q - 1].0 == t {
                    q -= 1;
                }
                pcl[q].2 as usize
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
                    let cc = c.2 as usize;
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
        i = j;
    }
}

fn heavy_threshold() -> usize {
    static T: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *T.get_or_init(|| std::env::var("SS_HEAVY").ok().and_then(|s| s.parse().ok()).unwrap_or(64))
}

/// Assists for one person via merged click intervals: a campaign assists a session starting
/// at s iff one of its clicks c has c <= s <= c + 7d. Sessions are sorted by start.
fn heavy_assists(pcl: &[(u64, Bytes, u16)], starts: &[i64], sdays: &[usize], ndays: usize, cacc: &mut [u64]) {
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
    let mut cc: Vec<(u16, i64)> = pcl.iter().map(|c| (c.2, (c.0 & 0xFFFF_FFFF) as i64)).collect();
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
