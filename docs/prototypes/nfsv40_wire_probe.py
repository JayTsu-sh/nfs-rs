#!/usr/bin/env python3
"""THROWAWAY raw-RPC NFSv4.0 lifecycle probe. Never ship this as client code."""
import argparse, os, random, socket, struct, time

OK=0; OP_CLOSE=4; OP_GETFH=10; OP_LOOKUP=15; OP_OPEN=18; OP_OPEN_CONFIRM=20
OP_PUTFH=22; OP_PUTROOTFH=24; OP_REMOVE=28; OP_SETCLIENTID=35
OP_SETCLIENTID_CONFIRM=36; OP_WRITE=38; OPEN4_RESULT_CONFIRM=2

def u32(v): return struct.pack(">I",v)
def u64(v): return struct.pack(">Q",v)
def opaque(v): return u32(len(v))+v+b"\0"*((-len(v))%4)
def string(v): return opaque(v.encode())
def op(n, body=b""): return u32(n)+body

class Reader:
    def __init__(self,b): self.b=b; self.i=0
    def take(self,n): v=self.b[self.i:self.i+n]; self.i+=n; return v
    def u32(self): return struct.unpack(">I",self.take(4))[0]
    def u64(self): return struct.unpack(">Q",self.take(8))[0]
    def opaque(self): n=self.u32(); v=self.take(n); self.take((-n)%4); return v

class Rpc:
    def __init__(self,host): self.host=host; self.xid=random.randrange(1,2**31); self.connect()
    def connect(self):
        self.s=socket.socket(); self.s.settimeout(10); self.s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        ports=list(range(700,1024)); random.shuffle(ports)
        for p in ports:
            try: self.s.bind(("0.0.0.0",p)); self.s.connect((self.host,2049)); self.port=p; return
            except OSError:
                self.s.close(); self.s=socket.socket(); self.s.settimeout(10); self.s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        raise RuntimeError("no privileged source port")
    def close(self): self.s.close()
    def call(self,payload):
        self.xid+=1; machine=b"nfsrs-v40-probe"; auth=u32(int(time.time()))+opaque(machine)+u32(0)+u32(0)+u32(0)
        msg=u32(self.xid)+u32(0)+u32(2)+u32(100003)+u32(4)+u32(1)+u32(1)+opaque(auth)+u32(0)+u32(0)+payload
        self.s.sendall(u32(0x80000000|len(msg))+msg)
        mark=self._recv(4); length=struct.unpack(">I",mark)[0]&0x7fffffff; r=Reader(self._recv(length))
        assert r.u32()==self.xid and r.u32()==1 and r.u32()==0
        r.u32(); r.take(r.u32()); r.take((-r.i)%4); assert r.u32()==0
        return r
    def _recv(self,n):
        out=b""
        while len(out)<n:
            chunk=self.s.recv(n-len(out))
            if not chunk: raise EOFError("RPC connection closed")
            out+=chunk
        return out

def compound(rpc,ops,tag="nfsrs-v40-probe"):
    r=rpc.call(string(tag)+u32(0)+u32(len(ops))+b"".join(ops))
    status=r.u32(); r.opaque(); count=r.u32()
    if status!=OK: raise RuntimeError(f"COMPOUND failed NFS4 status={status}")
    return r,count
def result_status(r,want):
    got=r.u32(); status=r.u32()
    if got!=want or status!=OK: raise RuntimeError(f"op result want={want} got={got} status={status}")

def setclientid(rpc,identity,verifier):
    callback=u32(0)+string("")+string("")+u32(0)
    r,n=compound(rpc,[op(OP_SETCLIENTID,verifier+opaque(identity)+callback)])
    assert n==1; result_status(r,OP_SETCLIENTID); return r.u64(),r.take(8)
def confirm(rpc,clientid,verifier):
    r,n=compound(rpc,[op(OP_SETCLIENTID_CONFIRM,u64(clientid)+verifier)])
    assert n==1; result_status(r,OP_SETCLIENTID_CONFIRM)
def export_fh(rpc,path):
    ops=[op(OP_PUTROOTFH)]+[op(OP_LOOKUP,string(c)) for c in path.strip("/").split("/") if c]+[op(OP_GETFH)]
    r,n=compound(rpc,ops); assert n==len(ops)
    result_status(r,OP_PUTROOTFH)
    for _ in ops[1:-1]: result_status(r,OP_LOOKUP)
    result_status(r,OP_GETFH); return r.opaque()
def open_create(rpc,dirfh,clientid,owner,name):
    # OPEN seqid=0, BOTH access, deny none, UNCHECKED create with empty attrs, CLAIM_NULL.
    args=u32(0)+u32(3)+u32(0)+u64(clientid)+opaque(owner)+u32(1)+u32(0)+u32(0)+opaque(b"")+u32(0)+string(name)
    r,n=compound(rpc,[op(OP_PUTFH,opaque(dirfh)),op(OP_OPEN,args),op(OP_GETFH)]); assert n==3
    result_status(r,OP_PUTFH); result_status(r,OP_OPEN)
    stateid=r.take(16); r.u32(); r.u64(); r.u64(); flags=r.u32()
    words=r.u32()
    for _ in range(words): r.u32()
    delegation=r.u32()
    if delegation!=0: raise RuntimeError(f"unexpected delegation type {delegation}; callback was disabled")
    result_status(r,OP_GETFH); return stateid,r.opaque(),flags
def open_confirm(rpc,fh,stateid,seqid):
    r,n=compound(rpc,[op(OP_PUTFH,opaque(fh)),op(OP_OPEN_CONFIRM,u32(seqid)+stateid)]); assert n==2
    result_status(r,OP_PUTFH); result_status(r,OP_OPEN_CONFIRM); return r.take(16)
def write(rpc,fh,stateid,data):
    args=stateid+u64(0)+u32(2)+opaque(data)
    r,n=compound(rpc,[op(OP_PUTFH,opaque(fh)),op(OP_WRITE,args)]); assert n==2
    result_status(r,OP_PUTFH); result_status(r,OP_WRITE)
    return r.u32(),r.u32(),r.take(8)
def close_file(rpc,fh,stateid,seqid):
    r,n=compound(rpc,[op(OP_PUTFH,opaque(fh)),op(OP_CLOSE,u32(seqid)+stateid)]); assert n==2
    result_status(r,OP_PUTFH); result_status(r,OP_CLOSE); return r.take(16)
def remove(rpc,dirfh,name):
    r,n=compound(rpc,[op(OP_PUTFH,opaque(dirfh)),op(OP_REMOVE,string(name))]); assert n==2
    result_status(r,OP_PUTFH); result_status(r,OP_REMOVE)
    return bool(r.u32()),r.u64(),r.u64()

def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--server",default="10.128.61.200"); ap.add_argument("--export",default="/nfsrs_v40_test")
    a=ap.parse_args(); name=f"wire-probe-{os.getpid()}-{int(time.time())}"; identity=f"nfs-rs-prototype:{socket.gethostname()}:{os.getpid()}".encode(); verifier=os.urandom(8); owner=b"migration-owner-0"
    rpc=Rpc(a.server); print(f"connected server={a.server}:2049 source_port={rpc.port} minorversion=0 session_state=absent")
    try:
        clientid,confirmverf=setclientid(rpc,identity,verifier); print(f"SETCLIENTID clientid=0x{clientid:016x} state=unconfirmed")
        confirm(rpc,clientid,confirmverf); print("SETCLIENTID_CONFIRM state=confirmed epoch=1")
        dirfh=export_fh(rpc,a.export); print(f"namespace export={a.export} fh={dirfh.hex()}")
        stateid,fh,flags=open_create(rpc,dirfh,clientid,owner,name); seqid=1; print(f"OPEN owner_seqid=0 stateid={stateid.hex()} confirm_required={bool(flags&OPEN4_RESULT_CONFIRM)}")
        if flags&OPEN4_RESULT_CONFIRM:
            stateid=open_confirm(rpc,fh,stateid,seqid); seqid+=1; print(f"OPEN_CONFIRM owner_seqid={seqid-1} stateid={stateid.hex()}")
        data=b"nfs-rs NFSv4.0 raw wire lifecycle\n"; count,stable,writeverf=write(rpc,fh,stateid,data); print(f"WRITE count={count} committed={stable} verifier={writeverf.hex()}")
        old_port=rpc.port; rpc.close(); rpc=Rpc(a.server); print(f"reconnected old_source_port={old_port} new_source_port={rpc.port} clientid_unchanged=0x{clientid:016x}")
        final_stateid=close_file(rpc,fh,stateid,seqid); print(f"CLOSE owner_seqid={seqid} returned_stateid={final_stateid.hex()}")
        atomic,before,after=remove(rpc,dirfh,name); print(f"REMOVE cleanup=true atomic={atomic} change={before}->{after}")
        print("VERDICT: lifecycle succeeded with minorversion=0 and no session/slot/SEQUENCE state")
    finally: rpc.close()
if __name__=="__main__": main()
