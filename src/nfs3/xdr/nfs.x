/*
 * NFS Version 3 Protocol XDR Definition
 * Based on RFC 1813
 */

const NFS3_FHSIZE         = 64;
const NFS3_COOKIEVERFSIZE = 8;
const NFS3_CREATEVERFSIZE = 8;
const NFS3_WRITEVERFSIZE  = 8;

const FHSIZE2       = 32;
const MAXNAMLEN2    = 255;
const MAXPATHLEN2   = 1024;
const NFSCOOKIESIZE2 = 4;
const NFSMAXDATA2   = 8192;
const TRUE  = 1;
const FALSE = 0;

const ACCESS3_READ    = 1;
const ACCESS3_LOOKUP  = 2;
const ACCESS3_MODIFY  = 4;
const ACCESS3_EXTEND  = 8;
const ACCESS3_DELETE  = 16;
const ACCESS3_EXECUTE = 32;

const FSF3_LINK        = 1;
const FSF3_SYMLINK     = 2;
const FSF3_HOMOGENEOUS = 8;
const FSF3_CANSETTIME  = 16;

/* Basic typedefs */
typedef opaque nfs_fh3<NFS3_FHSIZE>;
typedef opaque filename3<MAXNAMLEN2>;
typedef opaque nfspath3<MAXPATHLEN2>;

typedef unsigned hyper fileid3;
typedef unsigned hyper cookie3;
typedef opaque   cookieverf3[NFS3_COOKIEVERFSIZE];
typedef opaque   createverf3[NFS3_CREATEVERFSIZE];
typedef opaque   writeverf3[NFS3_WRITEVERFSIZE];

typedef unsigned int  uid3;
typedef unsigned int  gid3;
typedef unsigned hyper size3;
typedef unsigned hyper offset3;
typedef unsigned int  mode3;
typedef unsigned int  count3;

/* Status codes */
enum nfsstat3 {
    NFS3_OK             = 0,
    NFS3ERR_PERM        = 1,
    NFS3ERR_NOENT       = 2,
    NFS3ERR_IO          = 5,
    NFS3ERR_NXIO        = 6,
    NFS3ERR_ACCES       = 13,
    NFS3ERR_EXIST       = 17,
    NFS3ERR_XDEV        = 18,
    NFS3ERR_NODEV       = 19,
    NFS3ERR_NOTDIR      = 20,
    NFS3ERR_ISDIR       = 21,
    NFS3ERR_INVAL       = 22,
    NFS3ERR_FBIG        = 27,
    NFS3ERR_NOSPC       = 28,
    NFS3ERR_ROFS        = 30,
    NFS3ERR_MLINK       = 31,
    NFS3ERR_NAMETOOLONG = 63,
    NFS3ERR_NOTEMPTY    = 66,
    NFS3ERR_DQUOT       = 69,
    NFS3ERR_STALE       = 70,
    NFS3ERR_REMOTE      = 71,
    NFS3ERR_BADHANDLE   = 10001,
    NFS3ERR_NOT_SYNC    = 10002,
    NFS3ERR_BAD_COOKIE  = 10003,
    NFS3ERR_NOTSUPP     = 10004,
    NFS3ERR_TOOSMALL    = 10005,
    NFS3ERR_SERVERFAULT = 10006,
    NFS3ERR_BADTYPE     = 10007,
    NFS3ERR_JUKEBOX     = 10008
};

enum ftype3 {
    NF3REG  = 1,
    NF3DIR  = 2,
    NF3BLK  = 3,
    NF3CHR  = 4,
    NF3LNK  = 5,
    NF3SOCK = 6,
    NF3FIFO = 7
};

struct specdata3 {
    unsigned int specdata1;
    unsigned int specdata2;
};

struct nfstime3 {
    unsigned int seconds;
    unsigned int nseconds;
};

struct fattr3 {
    ftype3       type;
    mode3        mode;
    unsigned int nlink;
    uid3         uid;
    gid3         gid;
    size3        size;
    size3        used;
    specdata3    rdev;
    unsigned hyper fsid;
    fileid3      fileid;
    nfstime3     atime;
    nfstime3     mtime;
    nfstime3     ctime;
};

union post_op_attr switch (bool attributes_follow) {
    case TRUE:
        fattr3 attributes;
    case FALSE:
        void;
};

struct wcc_attr {
    size3    size;
    nfstime3 mtime;
    nfstime3 ctime;
};

union pre_op_attr switch (bool attributes_follow) {
    case TRUE:
        wcc_attr attributes;
    case FALSE:
        void;
};

struct wcc_data {
    pre_op_attr before;
    post_op_attr after;
};

union post_op_fh3 switch (bool handle_follows) {
    case TRUE:
        nfs_fh3 handle;
    case FALSE:
        void;
};

struct diropargs3 {
    nfs_fh3   dir;
    filename3 name;
};

enum stable_how {
    UNSTABLE  = 0,
    DATA_SYNC = 1,
    FILE_SYNC = 2
};

union sattrguard3 switch (bool check) {
    case TRUE:
        nfstime3 obj_ctime;
    case FALSE:
        void;
};

union set_mode3 switch (bool set_it) {
    case TRUE:
        mode3 mode;
    default:
        void;
};

union set_uid3 switch (bool set_it) {
    case TRUE:
        uid3 uid;
    default:
        void;
};

union set_gid3 switch (bool set_it) {
    case TRUE:
        gid3 gid;
    default:
        void;
};

union set_size3 switch (bool set_it) {
    case TRUE:
        size3 size;
    default:
        void;
};

enum time_how {
    DONT_CHANGE        = 0,
    SET_TO_SERVER_TIME = 1,
    SET_TO_CLIENT_TIME = 2
};

union set_atime switch (time_how set_it) {
    case SET_TO_CLIENT_TIME:
        nfstime3 atime;
    default:
        void;
};

union set_mtime switch (time_how set_it) {
    case SET_TO_CLIENT_TIME:
        nfstime3 mtime;
    default:
        void;
};

struct sattr3 {
    set_mode3 mode;
    set_uid3  uid;
    set_gid3  gid;
    set_size3 size;
    set_atime atime;
    set_mtime mtime;
};

enum createmode3 {
    UNCHECKED = 0,
    GUARDED   = 1,
    EXCLUSIVE = 2
};

union createhow3 switch (createmode3 mode) {
    case UNCHECKED:
        sattr3      obj_attributes;
    case GUARDED:
        sattr3      g_obj_attributes;
    case EXCLUSIVE:
        createverf3 verf;
};

struct devicedata3 {
    sattr3    dev_attributes;
    specdata3 spec;
};

union mknoddata3 switch (ftype3 type) {
    case NF3CHR:
        devicedata3 chr_device;
    case NF3BLK:
        devicedata3 blk_device;
    case NF3SOCK:
        sattr3 sock_attributes;
    case NF3FIFO:
        sattr3 pipe_attributes;
    default:
        void;
};

struct symlinkdata3 {
    sattr3    symlink_attributes;
    nfspath3  symlink_data;
};

struct entry3 {
    fileid3   fileid;
    filename3 name;
    cookie3   cookie;
    entry3    *nextentry;
};

struct dirlist3 {
    entry3 *entries;
    bool   eof;
};

struct entryplus3 {
    fileid3      fileid;
    filename3    name;
    cookie3      cookie;
    post_op_attr name_attributes;
    post_op_fh3  name_handle;
    entryplus3   *nextentry;
};

struct dirlistplus3 {
    entryplus3 *entries;
    bool       eof;
};

/* GETATTR */
struct GETATTR3args {
    nfs_fh3 object;
};
struct GETATTR3resok {
    fattr3 obj_attributes;
};
union GETATTR3res switch (nfsstat3 status) {
    case NFS3_OK:
        GETATTR3resok resok;
    default:
        void;
};

/* SETATTR */
struct SETATTR3resok {
    wcc_data obj_wcc;
};
union SETATTR3res switch (nfsstat3 status) {
    case NFS3_OK:
        SETATTR3resok resok;
    default:
        void;
};

/* LOOKUP */
struct LOOKUP3args {
    diropargs3 what;
};
struct LOOKUP3resok {
    nfs_fh3      object;
    post_op_attr obj_attributes;
    post_op_attr dir_attributes;
};
union LOOKUP3res switch (nfsstat3 status) {
    case NFS3_OK:
        LOOKUP3resok resok;
    default:
        void;
};

/* ACCESS */
struct ACCESS3args {
    nfs_fh3      object;
    unsigned int access;
};
struct ACCESS3resok {
    post_op_attr obj_attributes;
    unsigned int access;
};
union ACCESS3res switch (nfsstat3 status) {
    case NFS3_OK:
        ACCESS3resok resok;
    default:
        void;
};

/* READLINK */
struct READLINK3args {
    nfs_fh3 symlink;
};
struct READLINK3resok {
    post_op_attr symlink_attributes;
    nfspath3     data;
};
union READLINK3res switch (nfsstat3 status) {
    case NFS3_OK:
        READLINK3resok resok;
    default:
        void;
};

/* READ */
struct READ3args {
    nfs_fh3  file;
    offset3  offset;
    count3   count;
};
struct READ3resok {
    post_op_attr file_attributes;
    count3       count;
    bool         eof;
    opaque       data<>;
};
union READ3res switch (nfsstat3 status) {
    case NFS3_OK:
        READ3resok resok;
    default:
        void;
};

/* WRITE */
struct WRITE3args {
    nfs_fh3    file;
    offset3    offset;
    count3     count;
    stable_how stable;
    opaque     data<>;
};
struct WRITE3resok {
    wcc_data   file_wcc;
    count3     count;
    stable_how committed;
    writeverf3 verf;
};
union WRITE3res switch (nfsstat3 status) {
    case NFS3_OK:
        WRITE3resok resok;
    default:
        void;
};

/* CREATE */
struct CREATE3args {
    diropargs3 where;
    createhow3 how;
};
struct CREATE3resok {
    post_op_fh3  obj;
    post_op_attr obj_attributes;
    wcc_data     dir_wcc;
};
union CREATE3res switch (nfsstat3 status) {
    case NFS3_OK:
        CREATE3resok resok;
    default:
        void;
};

/* MKDIR */
struct MKDIR3args {
    diropargs3 where;
    sattr3     attributes;
};
struct MKDIR3resok {
    post_op_fh3  obj;
    post_op_attr obj_attributes;
    wcc_data     dir_wcc;
};
union MKDIR3res switch (nfsstat3 status) {
    case NFS3_OK:
        MKDIR3resok resok;
    default:
        void;
};

/* SYMLINK */
struct SYMLINK3args {
    diropargs3   where;
    symlinkdata3 symlink;
};
struct SYMLINK3resok {
    post_op_fh3  obj;
    post_op_attr obj_attributes;
    wcc_data     dir_wcc;
};
union SYMLINK3res switch (nfsstat3 status) {
    case NFS3_OK:
        SYMLINK3resok resok;
    default:
        void;
};

/* MKNOD */
struct MKNOD3args {
    diropargs3 where;
    mknoddata3 what;
};
struct MKNOD3resok {
    post_op_fh3  obj;
    post_op_attr obj_attributes;
    wcc_data     dir_wcc;
};
union MKNOD3res switch (nfsstat3 status) {
    case NFS3_OK:
        MKNOD3resok resok;
    default:
        void;
};

/* REMOVE */
struct REMOVE3args {
    diropargs3 object;
};
struct REMOVE3resok {
    wcc_data dir_wcc;
};
union REMOVE3res switch (nfsstat3 status) {
    case NFS3_OK:
        REMOVE3resok resok;
    default:
        void;
};

/* RMDIR */
struct RMDIR3args {
    diropargs3 object;
};
struct RMDIR3resok {
    wcc_data dir_wcc;
};
union RMDIR3res switch (nfsstat3 status) {
    case NFS3_OK:
        RMDIR3resok resok;
    default:
        void;
};

/* RENAME */
struct RENAME3args {
    diropargs3 from;
    diropargs3 to;
};
struct RENAME3resok {
    wcc_data fromdir_wcc;
    wcc_data todir_wcc;
};
union RENAME3res switch (nfsstat3 status) {
    case NFS3_OK:
        RENAME3resok resok;
    default:
        void;
};

/* LINK */
struct LINK3args {
    nfs_fh3    file;
    diropargs3 link;
};
struct LINK3resok {
    post_op_attr file_attributes;
    wcc_data     linkdir_wcc;
};
union LINK3res switch (nfsstat3 status) {
    case NFS3_OK:
        LINK3resok resok;
    default:
        void;
};

/* READDIR */
struct READDIR3args {
    nfs_fh3     dir;
    cookie3     cookie;
    cookieverf3 cookieverf;
    count3      count;
};
struct READDIR3resok {
    post_op_attr dir_attributes;
    cookieverf3  cookieverf;
    dirlist3     reply;
};
union READDIR3res switch (nfsstat3 status) {
    case NFS3_OK:
        READDIR3resok resok;
    default:
        void;
};

/* READDIRPLUS */
struct READDIRPLUS3args {
    nfs_fh3     dir;
    cookie3     cookie;
    cookieverf3 cookieverf;
    count3      dircount;
    count3      maxcount;
};
struct READDIRPLUS3resok {
    post_op_attr  dir_attributes;
    cookieverf3   cookieverf;
    dirlistplus3  reply;
};
union READDIRPLUS3res switch (nfsstat3 status) {
    case NFS3_OK:
        READDIRPLUS3resok resok;
    default:
        void;
};

/* FSSTAT */
struct FSSTAT3args {
    nfs_fh3 fsroot;
};
struct FSSTAT3resok {
    post_op_attr obj_attributes;
    size3        tbytes;
    size3        fbytes;
    size3        abytes;
    size3        tfiles;
    size3        ffiles;
    size3        afiles;
    unsigned int invarsec;
};
union FSSTAT3res switch (nfsstat3 status) {
    case NFS3_OK:
        FSSTAT3resok resok;
    default:
        void;
};

/* FSINFO */
struct FSINFO3args {
    nfs_fh3 fsroot;
};
struct FSINFO3resok {
    post_op_attr obj_attributes;
    unsigned int rtmax;
    unsigned int rtpref;
    unsigned int rtmult;
    unsigned int wtmax;
    unsigned int wtpref;
    unsigned int wtmult;
    unsigned int dtpref;
    size3        maxfilesize;
    nfstime3     time_delta;
    unsigned int properties;
};
union FSINFO3res switch (nfsstat3 status) {
    case NFS3_OK:
        FSINFO3resok resok;
    default:
        void;
};

/* PATHCONF */
struct PATHCONF3args {
    nfs_fh3 object;
};
struct PATHCONF3resok {
    post_op_attr obj_attributes;
    unsigned int linkmax;
    unsigned int name_max;
    bool         no_trunc;
    bool         chown_restricted;
    bool         case_insensitive;
    bool         case_preserving;
};
union PATHCONF3res switch (nfsstat3 status) {
    case NFS3_OK:
        PATHCONF3resok resok;
    default:
        void;
};

/* COMMIT */
struct COMMIT3args {
    nfs_fh3  file;
    offset3  offset;
    count3   count;
};
struct COMMIT3resok {
    wcc_data   file_wcc;
    writeverf3 verf;
};
union COMMIT3res switch (nfsstat3 status) {
    case NFS3_OK:
        COMMIT3resok resok;
    default:
        void;
};
