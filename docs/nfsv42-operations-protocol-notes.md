# NFSv4.2 operations: implementation notes

These notes cover sparse operations, CLONE, synchronous intra-server COPY,
and RFC 8276 xattrs. Physical AFX results belong in validation evidence;
product documentation alone does not establish the configured capabilities.

## Wire layout

Notation: `s=stateid4[16]`, `o/n=u64`, `c=u32`, `b=XDR bool`,
`v=opaque<>`, `k=component4`, `[]` is an XDR counted array. Every result
starts with `nfsstat4`; the table gives its successful payload. Scalars use
network byte order and variable opaque/string data has four-byte padding.
COPY's error arm is an exception described below.

| Operation | Opcode | Ordered arguments after opcode | Successful payload |
|---|---:|---|---|
| ALLOCATE | 59 | `s,o,n` | none |
| COPY | 60 | `src_s,dst_s,src_o,dst_o,n,consecutive:b,synchronous:b,source_servers[]` | `callback_s[0..1],count:u64,committed:u32,verifier[8],consecutive:b,synchronous:b` |
| DEALLOCATE | 62 | `s,o,n` | none |
| READ_PLUS | 68 | `s,o,c` | `eof:b,contents[]` |
| SEEK | 69 | `s,o,what:u32` | `eof:b,offset:u64` |
| CLONE | 71 | `src_s,dst_s,src_o,dst_o,n` | none |

READ_PLUS content: `kind:u32`, then DATA=0: `offset:u64,data:v`;
HOLE=1: `offset:u64,length:u64`. SEEK uses those same kind numbers.
COPY/CLONE use SAVED_FH=source, CURRENT_FH=destination; sequence:
`SEQUENCE,PUTFH(src),SAVEFH,PUTFH(dst),operation`.
[RFC 7863 section 3](https://www.rfc-editor.org/rfc/rfc7863.html#section-3).

New status discriminants: PARTNER_NOTSUPP=10088, PARTNER_NO_AUTH=10089,
UNION_NOTSUPP=10090, OFFLOAD_DENIED=10091, WRONG_LFS=10092,
BADLABEL=10093, OFFLOAD_NO_REQS=10094.
[RFC 7863 nfsstat4](https://www.rfc-editor.org/rfc/rfc7863.html#section-3).

## Operation semantics

| Operation | Constraints and observable behavior |
|---|---|
| ALLOCATE | WRITE stateid; regular file; may extend size to offset+length; reservation failure: NOSPC. |
| DEALLOCATE | WRITE stateid; regular file; size unchanged; reads return zeros, including unaligned edges. |
| SEEK | READ stateid; no match: OK+eof; beyond EOF: NXIO; EOF has a virtual hole. |
| READ_PLUS | READ stateid; contiguous contents; short results allowed; whole holes may extend outside requested interval. |
| CLONE | READ source/WRITE destination; clone_blksize attribute77 alignment; final source-EOF fragment excepted; count0=EOF; same-file overlap forbidden; atomic. |
| COPY | Different regular files; READ source/WRITE destination; count0=EOF; source range cannot exceed EOF; destination may extend. |

For synchronous intra-server COPY, send `true,true,[]`. Unsupported requested
requirements produce `OFFLOAD_NO_REQS` with two boolean result fields.
Successful synchronous completion has no callback stateid; a nonempty one
means asynchronous work. Optional operations may return NOTSUPP.
[RFC 7862 sections 15.1–15.13](https://www.rfc-editor.org/rfc/rfc7862.html#section-15).

## Xattrs

| Operation | Opcode | Ordered arguments | Successful payload |
|---|---:|---|---|
| GETXATTR | 72 | `key:k` | `value:v` |
| SETXATTR | 73 | `option:u32,key:k,value:v` | `change_info4` |
| LISTXATTRS | 74 | `cookie:u64,maxcount:u32` | `cookie:u64,names:k[],eof:b` |
| REMOVEXATTR | 75 | `key:k` | `change_info4` |

`change_info4=(atomic:b,before:u64,after:u64)`. No OPEN/stateid is needed.
SET options: EITHER=0, CREATE=1, REPLACE=2; CREATE-existing returns EXIST,
REPLACE-missing returns NOXATTR=10095. XATTR2BIG=10096 indicates filesystem
limits. Empty values are valid.

Query supported_attrs, then per-filesystem boolean xattr_support=82; send
xattr operations only when supported and true. Keys/values are opaque to
the protocol. Negotiated channel limits bound requests/responses;
LISTXATTRS maxcount includes resok XDR overhead, starts cookie0, has no
cookie verifier. ACCESS additions: XAREAD=0x40, XAWRITE=0x80, XALIST=0x100.
Attribute changes affect metadata/change; DELAY may accompany delegation
recall. [RFC 8276 sections 8.1–8.8](https://www.rfc-editor.org/rfc/rfc8276.html#section-8).

## Existing ACL and session contracts

ACL remains attribute12 and acl_support13, with the existing ordered ACE
array. supported_attrs remains attribute0 and is scoped by filesystem;
root capability observations cannot describe every nested filesystem.
Apply GETATTR/SETATTR to the target filehandle and retain server errors.
[RFC 8881 sections 5.8.1.1 and 6](https://www.rfc-editor.org/rfc/rfc8881.html#section-5.8.1.1).

Transport retries preserve the session, slot, sequence and original request;
session loss cannot establish whether a modifying request executed.
[RFC 8881 sections 2.10.6 and 2.10.13](https://www.rfc-editor.org/rfc/rfc8881.html#section-2.10.6).

Implementation consequences (design recommendations):

- Keep modifying requests owned after caller cancellation; retain both
  OPEN references for COPY/CLONE until settlement. Serialize barriers in a
  stable filehandle order to avoid deadlocks, including identical handles.
- Set cachethis for modifying operations. Preserve uncertainty on malformed
  or missing results; never replay them using a fresh sequence after an
  ambiguous failure. A cancelled caller is not evidence of rollback.
- Decode COPY committed/verifier and integrate durability with COMMIT.
  Reject unexpected asynchronous success and preserve uncertainty; attempt
  OFFLOAD_CANCEL using its destination filehandle and returned stateid.
  Do not add COPY_NOTIFY for an intra-server request.
- Bound all decoded counts and arithmetic. Clip expanded holes before
  materialization; reject gaps, overlaps, unknown arms and zero progress.
  Validate LISTXATTRS progress without interpreting the opaque cookie.
- Reuse pNFS write settlement before range mutation/offload so delayed DS
  writes cannot overwrite the new range. Prefer MDS for initial optional
  operations; DS support is independently negotiated.
- Test raw wire replies and adversarial status/length combinations before
  AFX. Record each unsupported optional operation as unsupported.

## ONTAP/AFX interpretation

NetApp documents RFC8276 xattrs on regular files/directories since ONTAP
9.12.1, distinct from unsupported NFSv4 named attributes. OPENATTR is
therefore not an xattr probe. Historical TR-4962 limits are 128 attributes,
255-byte keys, and 1729 combined key/value bytes. Verify current AFX limits
separately. [NetApp TR-4962 pages 3–5](https://www.netapp.com/pdf.html?item=/media/84595-tr-4962.pdf&v=20234241312).

ONTAP exposes independent `v4.2-xattrs` and `v4.2-sparsefile-ops` options;
the latter defaults disabled and requires advanced CLI privilege. A field
missing at admin privilege is not evidence of a disabled capability.
Start with read-only configuration queries;
do not equate protocol-version availability with optional-operation support.
[ONTAP 9.19.1 NFS CLI](https://docs.netapp.com/us-en/ontap-cli/vserver-nfs-create.html).

NetApp documents whole-file DEALLOCATE support, with partial deallocation
unsupported. The enabled AFX run reproduced EIO for a partial range with both
nfs-rs and the Linux kernel client; whole-file ranges passed. Preserve that
server error instead of treating it as success or implementing a zero-write
fallback. [NetApp DEALLOCATE limitation](https://kb.netapp.com/on-prem/ontap/da/NAS/NAS-KBs/Does_this_v4.2-sparsefile-ops_perform_ALLOCATE_DEALLOCATE%3F).
