/*
 * NFS Mount Protocol XDR Definition
 * Based on RFC 1813
 */

const FHSIZE    = 32;
const FHSIZE3   = 64;
const MNTPATHLEN = 1024;
const MNTNAMLEN  = 255;

typedef opaque dirpath<MNTPATHLEN>;
typedef opaque name<MNTNAMLEN>;

enum mountstat3 {
    MNT3_OK             = 0,
    MNT3ERR_PERM        = 1,
    MNT3ERR_NOENT       = 2,
    MNT3ERR_IO          = 5,
    MNT3ERR_ACCES       = 13,
    MNT3ERR_NOTDIR      = 20,
    MNT3ERR_INVAL       = 22,
    MNT3ERR_NAMETOOLONG = 63,
    MNT3ERR_NOTSUPP     = 10004,
    MNT3ERR_SERVERFAULT = 10006
};

typedef opaque fhandle3<FHSIZE3>;

struct mountres3_ok {
    fhandle3 fhandle;
    int      auth_flavors<>;
};

union mountres3 switch (mountstat3 fhs_status) {
    case MNT3_OK:
        mountres3_ok mountinfo;
    default:
        void;
};
