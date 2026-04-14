/*
 * NFSv4.1 Protocol XDR Definition
 * Based on RFC 5661 (NFSv4.1) and RFC 7530 (NFSv4.0 base types)
 *
 * This file defines types needed for the nfs-rs NFSv4.1 client.
 * Types are organized by RFC section for traceability.
 */

/* ================================================================
 * Constants
 * ================================================================ */

const NFS4_FHSIZE           = 128;
const NFS4_VERIFIER_SIZE    = 8;
const NFS4_OPAQUE_LIMIT     = 1024;
const NFS4_SESSIONID_SIZE   = 16;
const NFS4_DEVICEID4_SIZE   = 16;
/* NFS4_INT64_MAX, NFS4_UINT64_MAX, etc. omitted (overflow u32 in fastxdr) */

/* Access mask bits (RFC 7530 §6.2.1) */
const ACCESS4_READ          = 0x00000001;
const ACCESS4_LOOKUP        = 0x00000002;
const ACCESS4_MODIFY        = 0x00000004;
const ACCESS4_EXTEND        = 0x00000008;
const ACCESS4_DELETE        = 0x00000010;
const ACCESS4_EXECUTE       = 0x00000020;

/* OPEN4 share access/deny (RFC 5661 §18.16) */
const OPEN4_SHARE_ACCESS_READ   = 0x00000001;
const OPEN4_SHARE_ACCESS_WRITE  = 0x00000002;
const OPEN4_SHARE_ACCESS_BOTH   = 0x00000003;
const OPEN4_SHARE_DENY_NONE     = 0x00000000;
const OPEN4_SHARE_DENY_READ     = 0x00000001;
const OPEN4_SHARE_DENY_WRITE    = 0x00000002;
const OPEN4_SHARE_DENY_BOTH     = 0x00000003;

/* OPEN4_SHARE_ACCESS want delegation bits */
const OPEN4_SHARE_ACCESS_WANT_DELEG_MASK     = 0xFF00;
const OPEN4_SHARE_ACCESS_WANT_NO_DELEG       = 0x0100;
const OPEN4_SHARE_ACCESS_WANT_READ_DELEG     = 0x0200;
const OPEN4_SHARE_ACCESS_WANT_WRITE_DELEG    = 0x0300;
const OPEN4_SHARE_ACCESS_WANT_ANY_DELEG      = 0x0400;
const OPEN4_SHARE_ACCESS_WANT_NO_PREFERENCE  = 0x0500;
const OPEN4_SHARE_ACCESS_WANT_CANCEL         = 0x0600;
const OPEN4_SHARE_ACCESS_WANT_SIGNAL_DELEG_WHEN_RESRC_AVAIL = 0x10000;
const OPEN4_SHARE_ACCESS_WANT_PUSH_DELEG_WHEN_UNCONTESTED   = 0x20000;

/* createmode4 values defined via enum below */

/* SEQUENCE status flags (RFC 5661 §18.46) */
const SEQ4_STATUS_CB_PATH_DOWN                = 0x00000001;
const SEQ4_STATUS_CB_GSS_CONTEXTS_EXPIRING    = 0x00000002;
const SEQ4_STATUS_CB_GSS_CONTEXTS_EXPIRED     = 0x00000004;
const SEQ4_STATUS_EXPIRED_ALL_STATE_REVOKED   = 0x00000008;
const SEQ4_STATUS_EXPIRED_SOME_STATE_REVOKED  = 0x00000010;
const SEQ4_STATUS_ADMIN_STATE_REVOKED         = 0x00000020;
const SEQ4_STATUS_RECALLABLE_STATE_REVOKED    = 0x00000040;
const SEQ4_STATUS_LEASE_MOVED                 = 0x00000080;
const SEQ4_STATUS_RESTART_RECLAIM_NEEDED      = 0x00000100;
const SEQ4_STATUS_CB_PATH_DOWN_SESSION        = 0x00000200;
const SEQ4_STATUS_BACKCHANNEL_FAULT           = 0x00000400;
const SEQ4_STATUS_DEVID_CHANGED               = 0x00000800;
const SEQ4_STATUS_DEVID_DELETED               = 0x00001000;

/* EXCHANGE_ID flags (RFC 5661 §18.35) */
const EXCHGID4_FLAG_SUPP_MOVED_REFER          = 0x00000001;
const EXCHGID4_FLAG_SUPP_MOVED_MIGR           = 0x00000002;
const EXCHGID4_FLAG_SUPP_FENCE_OPS            = 0x00000004;
const EXCHGID4_FLAG_BIND_PRINC_STATEID        = 0x00000100;
const EXCHGID4_FLAG_USE_NON_PNFS              = 0x00010000;
const EXCHGID4_FLAG_USE_PNFS_MDS              = 0x00020000;
const EXCHGID4_FLAG_USE_PNFS_DS               = 0x00040000;
const EXCHGID4_FLAG_MASK_PNFS                 = 0x00070000;
const EXCHGID4_FLAG_UPD_CONFIRMED_REC_A       = 0x40000000;
const EXCHGID4_FLAG_CONFIRMED_R               = 0x80000000;

/* CREATE_SESSION flags (RFC 5661 §18.36) */
const CREATE_SESSION4_FLAG_PERSIST            = 0x00000001;
const CREATE_SESSION4_FLAG_CONN_BACK_CHAN      = 0x00000002;
const CREATE_SESSION4_FLAG_CONN_RDMA           = 0x00000004;

/* stable_how4 values defined via enum below */

/* ================================================================
 * Basic Types (RFC 7530 §2)
 * ================================================================ */

typedef opaque  nfs_fh4<NFS4_FHSIZE>;
typedef opaque  verifier4[NFS4_VERIFIER_SIZE];
typedef opaque  sessionid4[NFS4_SESSIONID_SIZE];
typedef unsigned hyper  clientid4;
typedef unsigned int    sequenceid4;
typedef unsigned int    slotid4;
typedef unsigned hyper  changeid4;
typedef opaque  utf8string<>;
typedef utf8string      component4;
typedef utf8string      utf8str_cs;
typedef utf8string      utf8str_cis;

/* Bitmap for fattr4 (variable-length array of uint32) */
typedef unsigned int bitmap4<>;

/* ================================================================
 * Status Codes (RFC 5661 §15.1)
 * ================================================================ */

enum nfsstat4 {
    NFS4_OK                             = 0,
    NFS4ERR_PERM                        = 1,
    NFS4ERR_NOENT                       = 2,
    NFS4ERR_IO                          = 5,
    NFS4ERR_NXIO                        = 6,
    NFS4ERR_ACCESS                      = 13,
    NFS4ERR_EXIST                       = 17,
    NFS4ERR_XDEV                        = 18,
    NFS4ERR_NOTDIR                      = 20,
    NFS4ERR_ISDIR                       = 21,
    NFS4ERR_INVAL                       = 22,
    NFS4ERR_FBIG                        = 27,
    NFS4ERR_NOSPC                       = 28,
    NFS4ERR_ROFS                        = 30,
    NFS4ERR_MLINK                       = 31,
    NFS4ERR_NAMETOOLONG                 = 63,
    NFS4ERR_NOTEMPTY                    = 66,
    NFS4ERR_DQUOT                       = 69,
    NFS4ERR_STALE                       = 70,
    NFS4ERR_BADHANDLE                   = 10001,
    NFS4ERR_BAD_COOKIE                  = 10003,
    NFS4ERR_NOTSUPP                     = 10004,
    NFS4ERR_TOOSMALL                    = 10005,
    NFS4ERR_SERVERFAULT                 = 10006,
    NFS4ERR_BADTYPE                     = 10007,
    NFS4ERR_DELAY                       = 10008,
    NFS4ERR_SAME                        = 10009,
    NFS4ERR_DENIED                      = 10010,
    NFS4ERR_EXPIRED                     = 10011,
    NFS4ERR_LOCKED                      = 10012,
    NFS4ERR_GRACE                       = 10013,
    NFS4ERR_FHEXPIRED                   = 10014,
    NFS4ERR_SHARE_DENIED                = 10015,
    NFS4ERR_WRONGSEC                    = 10016,
    NFS4ERR_CLID_INUSE                  = 10017,
    NFS4ERR_MOVED                       = 10019,
    NFS4ERR_NOFILEHANDLE                = 10020,
    NFS4ERR_MINOR_VERS_MISMATCH        = 10021,
    NFS4ERR_STALE_CLIENTID              = 10022,
    NFS4ERR_STALE_STATEID               = 10023,
    NFS4ERR_OLD_STATEID                 = 10024,
    NFS4ERR_BAD_STATEID                 = 10025,
    NFS4ERR_BAD_SEQID                   = 10026,
    NFS4ERR_NOT_SAME                    = 10027,
    NFS4ERR_LOCK_RANGE                  = 10028,
    NFS4ERR_SYMLINK                     = 10029,
    NFS4ERR_RESTOREFH                   = 10030,
    NFS4ERR_LEASE_MOVED                 = 10031,
    NFS4ERR_ATTRNOTSUPP                 = 10032,
    NFS4ERR_NO_GRACE                    = 10033,
    NFS4ERR_RECLAIM_BAD                 = 10034,
    NFS4ERR_RECLAIM_CONFLICT            = 10035,
    NFS4ERR_BADXDR                      = 10036,
    NFS4ERR_LOCKS_HELD                  = 10037,
    NFS4ERR_OPENMODE                    = 10038,
    NFS4ERR_BADOWNER                    = 10039,
    NFS4ERR_BADCHAR                     = 10040,
    NFS4ERR_BADNAME                     = 10041,
    NFS4ERR_BAD_RANGE                   = 10042,
    NFS4ERR_LOCK_NOTSUPP                = 10043,
    NFS4ERR_OP_ILLEGAL                  = 10044,
    NFS4ERR_DEADLOCK                    = 10045,
    NFS4ERR_FILE_OPEN                   = 10046,
    NFS4ERR_ADMIN_REVOKED               = 10047,
    NFS4ERR_CB_PATH_DOWN                = 10048,
    NFS4ERR_BADIOMODE                   = 10049,
    NFS4ERR_BADLAYOUT                   = 10050,
    NFS4ERR_BAD_SESSION_DIGEST          = 10051,
    NFS4ERR_BADSESSION                  = 10052,
    NFS4ERR_BADSLOT                     = 10053,
    NFS4ERR_COMPLETE_ALREADY            = 10054,
    NFS4ERR_CONN_NOT_BOUND_TO_SESSION   = 10055,
    NFS4ERR_DELEG_ALREADY_WANTED        = 10056,
    NFS4ERR_BACK_CHAN_BUSY              = 10057,
    NFS4ERR_LAYOUTTRYLATER              = 10058,
    NFS4ERR_LAYOUTUNAVAILABLE           = 10059,
    NFS4ERR_NOMATCHING_LAYOUT           = 10060,
    NFS4ERR_RECALLCONFLICT              = 10061,
    NFS4ERR_UNKNOWN_LAYOUTTYPE          = 10062,
    NFS4ERR_SEQ_MISORDERED              = 10063,
    NFS4ERR_SEQUENCE_POS                = 10064,
    NFS4ERR_REQ_TOO_BIG                 = 10065,
    NFS4ERR_REP_TOO_BIG                 = 10066,
    NFS4ERR_REP_TOO_BIG_TO_CACHE       = 10067,
    NFS4ERR_RETRY_UNCACHED_REP          = 10068,
    NFS4ERR_UNSAFE_COMPOUND             = 10069,
    NFS4ERR_TOO_MANY_OPS                = 10070,
    NFS4ERR_OP_NOT_IN_SESSION           = 10071,
    NFS4ERR_HASH_ALG_UNSUPP             = 10072,
    NFS4ERR_CLIENTID_BUSY               = 10074,
    NFS4ERR_PNFS_IO_HOLE                = 10075,
    NFS4ERR_SEQ_FALSE_RETRY             = 10076,
    NFS4ERR_BAD_HIGH_SLOT               = 10077,
    NFS4ERR_DEADSESSION                 = 10078,
    NFS4ERR_ENCR_ALG_UNSUPP             = 10079,
    NFS4ERR_PNFS_NO_LAYOUT              = 10080,
    NFS4ERR_NOT_ONLY_OP                 = 10081,
    NFS4ERR_WRONG_CRED                  = 10082,
    NFS4ERR_WRONG_TYPE                  = 10083,
    NFS4ERR_DIRDELEG_UNAVAIL            = 10084,
    NFS4ERR_REJECT_DELEG                = 10085,
    NFS4ERR_RETURNCONFLICT              = 10086,
    NFS4ERR_DELEG_REVOKED               = 10087
};

/* ================================================================
 * Core Structures (RFC 7530 §2)
 * ================================================================ */

struct stateid4 {
    unsigned int    seqid;
    opaque          other[12];
};

struct nfstime4 {
    hyper           seconds;
    unsigned int    nseconds;
};

enum nfs_ftype4 {
    NF4REG          = 1,
    NF4DIR          = 2,
    NF4BLK          = 3,
    NF4CHR          = 4,
    NF4LNK          = 5,
    NF4SOCK         = 6,
    NF4FIFO         = 7,
    NF4ATTRDIR      = 8,
    NF4NAMEDATTR    = 9
};

struct specdata4 {
    unsigned int    specdata1;
    unsigned int    specdata2;
};

/* fattr4: bitmap + opaque encoded attributes (RFC 7530 §3.3) */
struct fattr4 {
    bitmap4         attrmask;
    opaque          attr_vals<>;
};

/* change_info4 (RFC 7530 §2.1) */
struct change_info4 {
    bool            atomic;
    changeid4       before;
    changeid4       after;
};

/* ================================================================
 * SEQUENCE (RFC 5661 §18.46)
 * ================================================================ */

struct SEQUENCE4args {
    sessionid4      sa_sessionid;
    sequenceid4     sa_sequenceid;
    slotid4         sa_slotid;
    slotid4         sa_highest_slotid;
    bool            sa_cachethis;
};

struct SEQUENCE4resok {
    sessionid4      sr_sessionid;
    sequenceid4     sr_sequenceid;
    slotid4         sr_slotid;
    slotid4         sr_highest_slotid;
    slotid4         sr_target_highest_slotid;
    unsigned int    sr_status_flags;
};

union SEQUENCE4res switch (nfsstat4 status) {
    case NFS4_OK:
        SEQUENCE4resok  sr_resok4;
    default:
        void;
};

/* ================================================================
 * EXCHANGE_ID (RFC 5661 §18.35)
 * ================================================================ */

struct state_protect_ops4 {
    bitmap4     spo_must_enforce;
    bitmap4     spo_must_allow;
};

enum state_protect_how4 {
    SP4_NONE        = 0,
    SP4_MACH_CRED   = 1,
    SP4_SSV         = 2
};

union state_protect4_a switch (state_protect_how4 spa_how) {
    case SP4_NONE:
        void;
    case SP4_MACH_CRED:
        state_protect_ops4  spa_mach_ops;
    case SP4_SSV:
        void;
};

union state_protect4_r switch (state_protect_how4 spr_how) {
    case SP4_NONE:
        void;
    case SP4_MACH_CRED:
        state_protect_ops4  spr_mach_ops;
    case SP4_SSV:
        void;
};

struct nfs_impl_id4 {
    utf8str_cis     nii_domain;
    utf8str_cs      nii_name;
    nfstime4        nii_date;
};

struct client_owner4 {
    verifier4       co_verifier;
    opaque          co_ownerid<NFS4_OPAQUE_LIMIT>;
};

struct EXCHANGE_ID4args {
    client_owner4       eia_clientowner;
    unsigned int        eia_flags;
    state_protect4_a    eia_state_protect;
    nfs_impl_id4        eia_client_impl_id<1>;
};

struct server_owner4 {
    unsigned hyper      so_minor_id;
    opaque              so_major_id<NFS4_OPAQUE_LIMIT>;
};

struct EXCHANGE_ID4resok {
    clientid4           eir_clientid;
    sequenceid4         eir_sequenceid;
    unsigned int        eir_flags;
    state_protect4_r    eir_state_protect;
    server_owner4       eir_server_owner;
    opaque              eir_server_scope<NFS4_OPAQUE_LIMIT>;
    nfs_impl_id4        eir_server_impl_id<1>;
};

union EXCHANGE_ID4res switch (nfsstat4 status) {
    case NFS4_OK:
        EXCHANGE_ID4resok  eir_resok4;
    default:
        void;
};

/* ================================================================
 * CREATE_SESSION (RFC 5661 §18.36)
 * ================================================================ */

enum nfs_cb_opnum4 {
    OP_CB_GETATTR           = 3,
    OP_CB_RECALL            = 4,
    OP_CB_LAYOUTRECALL      = 5,
    OP_CB_NOTIFY            = 6,
    OP_CB_PUSH_DELEG        = 7,
    OP_CB_RECALL_ANY        = 8,
    OP_CB_RECALLABLE_OBJ_AVAIL = 9,
    OP_CB_RECALL_SLOT       = 10,
    OP_CB_SEQUENCE          = 11,
    OP_CB_WANTS_CANCELLED   = 12,
    OP_CB_NOTIFY_LOCK       = 13,
    OP_CB_NOTIFY_DEVICEID   = 14,
    OP_CB_ILLEGAL           = 10044
};

struct channel_attrs4 {
    unsigned int    ca_headerpadsize;
    unsigned int    ca_maxrequestsize;
    unsigned int    ca_maxresponsesize;
    unsigned int    ca_maxresponsesize_cached;
    unsigned int    ca_maxoperations;
    unsigned int    ca_maxrequests;
    unsigned int    ca_rdma_ird<1>;
};

typedef opaque callback_sec_parms4_raw<>;

struct CREATE_SESSION4args {
    clientid4               csa_clientid;
    sequenceid4             csa_sequence;
    unsigned int            csa_flags;
    channel_attrs4          csa_fore_chan_attrs;
    channel_attrs4          csa_back_chan_attrs;
    unsigned int            csa_cb_program;
    callback_sec_parms4_raw csa_sec_parms<>;
};

struct CREATE_SESSION4resok {
    sessionid4              csr_sessionid;
    sequenceid4             csr_sequence;
    unsigned int            csr_flags;
    channel_attrs4          csr_fore_chan_attrs;
    channel_attrs4          csr_back_chan_attrs;
};

union CREATE_SESSION4res switch (nfsstat4 status) {
    case NFS4_OK:
        CREATE_SESSION4resok    csr_resok4;
    default:
        void;
};

/* ================================================================
 * DESTROY_SESSION (RFC 5661 §18.37)
 * ================================================================ */

struct DESTROY_SESSION4args {
    sessionid4  dsa_sessionid;
};

union DESTROY_SESSION4res switch (nfsstat4 status) {
    case NFS4_OK:
        void;
    default:
        void;
};

/* ================================================================
 * RECLAIM_COMPLETE (RFC 5661 §18.51)
 * ================================================================ */

struct RECLAIM_COMPLETE4args {
    bool        rca_one_fs;
};

union RECLAIM_COMPLETE4res switch (nfsstat4 status) {
    case NFS4_OK:
        void;
    default:
        void;
};

/* ================================================================
 * File Handle Operations
 * ================================================================ */

/* PUTROOTFH (RFC 7530 §16.24) */
union PUTROOTFH4res switch (nfsstat4 status) {
    case NFS4_OK:
        void;
    default:
        void;
};

/* PUTFH (RFC 7530 §16.22) */
struct PUTFH4args {
    nfs_fh4     object;
};

union PUTFH4res switch (nfsstat4 status) {
    case NFS4_OK:
        void;
    default:
        void;
};

/* GETFH (RFC 7530 §16.10) */
struct GETFH4resok {
    nfs_fh4     object;
};

union GETFH4res switch (nfsstat4 status) {
    case NFS4_OK:
        GETFH4resok     resok4;
    default:
        void;
};

/* SAVEFH / RESTOREFH (RFC 7530 §16.25, §16.27) */
union SAVEFH4res switch (nfsstat4 status) {
    case NFS4_OK:
        void;
    default:
        void;
};

union RESTOREFH4res switch (nfsstat4 status) {
    case NFS4_OK:
        void;
    default:
        void;
};

/* ================================================================
 * LOOKUP (RFC 7530 §16.15)
 * ================================================================ */

struct LOOKUP4args {
    component4      objname;
};

union LOOKUP4res switch (nfsstat4 status) {
    case NFS4_OK:
        void;
    default:
        void;
};

/* LOOKUPP (RFC 7530 §16.16) */
union LOOKUPP4res switch (nfsstat4 status) {
    case NFS4_OK:
        void;
    default:
        void;
};

/* ================================================================
 * GETATTR / SETATTR (RFC 7530 §16.9, §16.30)
 * ================================================================ */

struct GETATTR4args {
    bitmap4     attr_request;
};

struct GETATTR4resok {
    fattr4      obj_attributes;
};

union GETATTR4res switch (nfsstat4 status) {
    case NFS4_OK:
        GETATTR4resok   resok4;
    default:
        void;
};

struct SETATTR4args {
    stateid4    sa_stateid;
    fattr4      sa_attribs;
};

struct SETATTR4res {
    nfsstat4    status;
    bitmap4     attrsset;
};

/* ================================================================
 * ACCESS (RFC 7530 §16.1)
 * ================================================================ */

struct ACCESS4args {
    unsigned int    access;
};

struct ACCESS4resok {
    unsigned int    supported;
    unsigned int    access;
};

union ACCESS4res switch (nfsstat4 status) {
    case NFS4_OK:
        ACCESS4resok    resok4;
    default:
        void;
};

/* ================================================================
 * READ / WRITE / COMMIT (RFC 7530 §16.25, §16.36, §16.3)
 * ================================================================ */

struct READ4args {
    stateid4        stateid;
    unsigned hyper  offset;
    unsigned int    count;
};

struct READ4resok {
    bool            eof;
    opaque          data<>;
};

union READ4res switch (nfsstat4 status) {
    case NFS4_OK:
        READ4resok  resok4;
    default:
        void;
};

enum stable_how4 {
    UNSTABLE4   = 0,
    DATA_SYNC4  = 1,
    FILE_SYNC4  = 2
};

struct WRITE4args {
    stateid4        stateid;
    unsigned hyper  offset;
    stable_how4     stable;
    opaque          data<>;
};

struct WRITE4resok {
    unsigned int    count;
    stable_how4     committed;
    verifier4       writeverf;
};

union WRITE4res switch (nfsstat4 status) {
    case NFS4_OK:
        WRITE4resok resok4;
    default:
        void;
};

struct COMMIT4args {
    unsigned hyper  offset;
    unsigned int    count;
};

struct COMMIT4resok {
    verifier4       writeverf;
};

union COMMIT4res switch (nfsstat4 status) {
    case NFS4_OK:
        COMMIT4resok    resok4;
    default:
        void;
};

/* ================================================================
 * OPEN / CLOSE (RFC 7530 §16.16, §16.3)
 * ================================================================ */

enum opentype4 {
    OPEN4_NOCREATE  = 0,
    OPEN4_CREATE    = 1
};

enum createmode4 {
    UNCHECKED4      = 0,
    GUARDED4        = 1,
    EXCLUSIVE4      = 2,
    EXCLUSIVE4_1    = 3
};

union createhow4 switch (createmode4 mode) {
    case UNCHECKED4:
        fattr4      createattrs;
    case GUARDED4:
        fattr4      createattrs;
    case EXCLUSIVE4:
        verifier4   createverf;
    case EXCLUSIVE4_1:
        verifier4   ch_createboth_verf;
};

union openflag4 switch (opentype4 opentype) {
    case OPEN4_CREATE:
        createhow4  how;
    default:
        void;
};

enum open_delegation_type4 {
    OPEN_DELEGATE_NONE      = 0,
    OPEN_DELEGATE_READ      = 1,
    OPEN_DELEGATE_WRITE     = 2,
    OPEN_DELEGATE_NONE_EXT  = 3
};

enum open_claim_type4 {
    CLAIM_NULL              = 0,
    CLAIM_PREVIOUS          = 1,
    CLAIM_DELEGATE_CUR      = 2,
    CLAIM_DELEGATE_PREV     = 3,
    CLAIM_FH                = 4,
    CLAIM_DELEG_CUR_FH      = 5,
    CLAIM_DELEG_PREV_FH     = 6
};

struct open_claim_delegate_cur4 {
    stateid4    delegate_stateid;
    component4  file;
};

union open_claim4 switch (open_claim_type4 claim) {
    case CLAIM_NULL:
        component4      file;
    case CLAIM_PREVIOUS:
        open_delegation_type4   delegate_type;
    case CLAIM_DELEGATE_CUR:
        open_claim_delegate_cur4    delegate_cur_info;
    case CLAIM_DELEGATE_PREV:
        component4      file_delegate_prev;
    case CLAIM_FH:
        void;
    case CLAIM_DELEG_CUR_FH:
        stateid4        oc_delegate_stateid;
    case CLAIM_DELEG_PREV_FH:
        void;
};

struct open_owner4 {
    clientid4       clientid;
    opaque          owner<NFS4_OPAQUE_LIMIT>;
};

struct OPEN4args {
    unsigned int    seqid;
    unsigned int    share_access;
    unsigned int    share_deny;
    open_owner4     owner;
    openflag4       openhow;
    open_claim4     claim;
};

struct open_read_delegation4 {
    stateid4    stateid;
    bool        recall;
    /* ace skipped — simplified for client use */
};

struct open_write_delegation4 {
    stateid4        stateid;
    bool            recall;
    /* space_limit and ace skipped */
};

enum why_no_delegation4 {
    WND4_NOT_WANTED         = 0,
    WND4_CONTENTION         = 1,
    WND4_RESOURCE           = 2,
    WND4_NOT_SUPP_FTYPE     = 3,
    WND4_WRITE_DELEG_NOT_SUPP_FTYPE = 4,
    WND4_NOT_SUPP_UPGRADE   = 5,
    WND4_NOT_SUPP_DOWNGRADE = 6,
    WND4_CANCELLED          = 7,
    WND4_IS_DIR             = 8
};

union open_none_delegation4 switch (why_no_delegation4 ond_why) {
    case WND4_CONTENTION:
        bool    ond_server_will_push_deleg;
    case WND4_RESOURCE:
        bool    ond_server_will_signal_avail;
    default:
        void;
};

union open_delegation4 switch (open_delegation_type4 delegation_type) {
    case OPEN_DELEGATE_NONE:
        void;
    case OPEN_DELEGATE_READ:
        open_read_delegation4   read;
    case OPEN_DELEGATE_WRITE:
        open_write_delegation4  write;
    case OPEN_DELEGATE_NONE_EXT:
        open_none_delegation4   od_whynone;
};

const OPEN4_RESULT_CONFIRM          = 0x00000002;
const OPEN4_RESULT_LOCKTYPE_POSIX   = 0x00000004;
const OPEN4_RESULT_PRESERVE_UNLINKED = 0x00000008;
const OPEN4_RESULT_MAY_NOTIFY_LOCK  = 0x00000020;

struct OPEN4resok {
    stateid4            stateid;
    change_info4        cinfo;
    unsigned int        rflags;
    bitmap4             attrset;
    open_delegation4    delegation;
};

union OPEN4res switch (nfsstat4 status) {
    case NFS4_OK:
        OPEN4resok  resok4;
    default:
        void;
};

/* CLOSE (RFC 7530 §16.3) */
struct CLOSE4args {
    unsigned int    seqid;
    stateid4        open_stateid;
};

union CLOSE4res switch (nfsstat4 status) {
    case NFS4_OK:
        stateid4    open_stateid;
    default:
        void;
};

/* ================================================================
 * READDIR (RFC 7530 §16.24)
 * ================================================================ */

typedef unsigned hyper nfs_cookie4;

struct READDIR4args {
    nfs_cookie4     cookie;
    verifier4       cookieverf;
    unsigned int    dircount;
    unsigned int    maxcount;
    bitmap4         attr_request;
};

struct entry4 {
    nfs_cookie4     cookie;
    component4      name;
    fattr4          attrs;
    entry4          *nextentry;
};

struct dirlist4 {
    entry4          *entries;
    bool            eof;
};

struct READDIR4resok {
    verifier4       cookieverf;
    dirlist4        reply;
};

union READDIR4res switch (nfsstat4 status) {
    case NFS4_OK:
        READDIR4resok   resok4;
    default:
        void;
};

/* ================================================================
 * READLINK (RFC 7530 §16.25)
 * ================================================================ */

struct READLINK4resok {
    component4      link;
};

union READLINK4res switch (nfsstat4 status) {
    case NFS4_OK:
        READLINK4resok  resok4;
    default:
        void;
};

/* ================================================================
 * CREATE / REMOVE / RENAME / LINK (RFC 7530)
 * ================================================================ */

struct CREATE4args {
    nfs_ftype4      objtype;
    component4      objname;
    fattr4          createattrs;
};

struct CREATE4resok {
    change_info4    cinfo;
    bitmap4         attrset;
};

union CREATE4res switch (nfsstat4 status) {
    case NFS4_OK:
        CREATE4resok    resok4;
    default:
        void;
};

struct REMOVE4args {
    component4      target;
};

struct REMOVE4resok {
    change_info4    cinfo;
};

union REMOVE4res switch (nfsstat4 status) {
    case NFS4_OK:
        REMOVE4resok    resok4;
    default:
        void;
};

struct RENAME4args {
    component4      oldname;
    component4      newname;
};

struct RENAME4resok {
    change_info4    source_cinfo;
    change_info4    target_cinfo;
};

union RENAME4res switch (nfsstat4 status) {
    case NFS4_OK:
        RENAME4resok    resok4;
    default:
        void;
};

struct LINK4args {
    component4      newname;
};

struct LINK4resok {
    change_info4    cinfo;
};

union LINK4res switch (nfsstat4 status) {
    case NFS4_OK:
        LINK4resok  resok4;
    default:
        void;
};

/* ================================================================
 * DESTROY_CLIENTID (RFC 5661 §18.50)
 * ================================================================ */

struct DESTROY_CLIENTID4args {
    clientid4   dca_clientid;
};

union DESTROY_CLIENTID4res switch (nfsstat4 status) {
    case NFS4_OK:
        void;
    default:
        void;
};
