# NFSv4 PATHCONF capability research

## Conclusion

Caching `supported_attrs` once from the mounted root is safe only while all
target filehandles remain in the same `fsid`. It is not a fully general
NFSv4 namespace solution because nested filesystems and referrals can cross
an `fsid` boundary.

The general one-RPC design is to include `supported_attrs` (attribute 0),
`fsid` (attribute 8), and the desired PATHCONF attributes in the same GETATTR.
The response bitmap and `supported_attrs` value then describe the target
object's filesystem without an extra round trip. A cache keyed by `fsid` can
be added as an optimization, but is not required for correctness.

Filtering the request bitmap is not sufficient by itself. The public
`Pathconf` type currently has non-optional fields, so it cannot distinguish
an actual `false`/`0` value from an unsupported or indeterminate value. A
general fix must expose availability explicitly (optional fields, a support
bitmap, or a new capability-aware result) rather than silently invent values.

## Evidence

- RFC 7530 sections 5.2 and 16.7 require clients to handle omitted
  RECOMMENDED attributes and specify that GETATTR returns only attributes the
  server can provide: <https://datatracker.ietf.org/doc/html/rfc7530#section-5.2>
  and <https://datatracker.ietf.org/doc/html/rfc7530#section-16.7>.
- Attribute 16 (`case_insensitive`) is RECOMMENDED, not REQUIRED:
  <https://datatracker.ietf.org/doc/html/rfc7530#section-5.8.2.3>.
- RFC 7530 section 5.8.1.1 scopes `supported_attrs` to objects with a matching
  `fsid`; section 5.4 classifies it as a per-filesystem attribute:
  <https://datatracker.ietf.org/doc/html/rfc7530#section-5.8.1.1> and
  <https://datatracker.ietf.org/doc/html/rfc7530#section-5.4>.
- ONTAP documents NFS names as case-sensitive and SMB names as
  case-insensitive but case-preserving, but this product behavior must not be
  used as a generic substitute for a missing protocol attribute:
  <https://docs.netapp.com/us-en/ontap/nfs-admin/case-sensitivity-file-directory-multiprotocol-concept.html>.

## Repository findings

- NFSv4.0 currently requires attributes 16, 17, 18, 28, 29, and 34 in
  `Mount::pathconf`, which is stricter than RFC 7530 for RECOMMENDED
  attributes.
- The mount-time parameter GETATTR can technically be extended to include
  attribute 0 without adding an RPC. That cache would still describe only the
  root filehandle's `fsid`.
- NFSv4.1 currently fabricates defaults for omitted PATHCONF attributes. This
  avoids failure but loses the distinction between a real value and an
  unsupported value, so it should not be copied as the generic semantic fix.

