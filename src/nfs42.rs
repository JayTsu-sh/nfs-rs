//! NFSv4.2 file operations (RFC 7862) and extended attributes (RFC 8276).
//!
//! File-handle operations require an existing OPEN with the appropriate access.
//! COPY is synchronous and intra-server; no client-side copy fallback is hidden.

use bytes::Bytes;

/// Content to locate with SEEK; distinct from a local file cursor seek.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum SeekContent {
    Data = 0,
    Hole = 1,
}

/// A server-selected content boundary. `eof` means no later match exists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SeekResult {
    pub eof: bool,
    pub offset: u64,
}

/// One READ_PLUS extent, clipped to the caller's requested interval.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadPlusContent {
    Data { offset: u64, data: Bytes },
    Hole { offset: u64, length: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadPlusResult {
    pub eof: bool,
    pub contents: Vec<ReadPlusContent>,
}

/// Result of a synchronous server-side COPY. The returned bytes are committed
/// before success; a short count is valid and is never silently retried.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CopyResult {
    pub count: u64,
}

/// RFC 8276 SETXATTR creation/replacement policy.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u32)]
pub enum XattrSetMode {
    #[default]
    Either = 0,
    Create = 1,
    Replace = 2,
}

/// Per-object/filesystem advertised attributes, not a promise that optional
/// operations such as COPY or SEEK are implemented. Probe those operations.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Nfs42Attributes {
    pub supported_attrs: Vec<u32>,
    pub clone_blksize: Option<u32>,
    pub xattr_support: bool,
}

impl Nfs42Attributes {
    pub fn supports_attribute(&self, attribute: u32) -> bool {
        self.supported_attrs
            .get(attribute as usize / 32)
            .is_some_and(|word| word & (1 << (attribute % 32)) != 0)
    }
}
