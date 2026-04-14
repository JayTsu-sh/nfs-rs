use std::env;
use std::fs;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);

    let nfs_xdr = fs::read_to_string("src/nfs3/xdr/nfs.x")?;
    let nfs_code = fastxdr::Generator::default()
        .generate(&nfs_xdr)?;
    // fastxdr wraps generated code in `mod xdr { ... }` (private). Make it public
    // so it can be re-exported from the parent module.
    let nfs_code = nfs_code.replacen("mod xdr {", "pub mod xdr {", 1);
    // fastxdr generates bool-typed discriminants matched against integer literals,
    // which is invalid in Rust 2021. Replace with bool literals.
    let nfs_code = nfs_code.replace("1 => Self::TRUE(", "true => Self::TRUE(");
    let nfs_code = nfs_code.replace("0 => Self::FALSE,", "false => Self::FALSE,");
    fs::write(out_dir.join("nfs_xdr.rs"), nfs_code)?;

    let mount_xdr = fs::read_to_string("src/nfs3/xdr/mount.x")?;
    let mount_code = fastxdr::Generator::default()
        .generate(&mount_xdr)?;
    let mount_code = mount_code.replacen("mod xdr {", "pub mod xdr {", 1);
    // fastxdr generates `try_variable_array::<i32>` for auth_flavors, but i32 does not
    // implement TryFrom<Bytes>. Replace with manual per-element decoding.
    let mount_code = mount_code.replace(
        "auth_flavors: v.try_variable_array::<i32>(None)?",
        "auth_flavors: { let n = v.try_u32()? as usize; let mut arr = Vec::with_capacity(n); for _ in 0..n { arr.push(v.try_i32()?); } arr }",
    );
    fs::write(out_dir.join("mount_xdr.rs"), mount_code)?;

    // NFSv4.1 XDR types (RFC 5661)
    let nfs4_xdr = fs::read_to_string("src/nfs41/xdr/nfs4.x")?;
    let nfs4_code = fastxdr::Generator::default()
        .generate(&nfs4_xdr)?;
    let nfs4_code = nfs4_code.replacen("mod xdr {", "pub mod xdr {", 1);
    // Add Copy+Clone to nfsstat4 enum (simple discriminant-only enum).
    let nfs4_code = nfs4_code.replace(
        "#[derive(Debug, PartialEq)]\npub enum nfsstat4",
        "#[derive(Debug, PartialEq, Clone, Copy)]\npub enum nfsstat4",
    );
    // Also add Copy+Clone to other simple enums used in match patterns.
    let nfs4_code = nfs4_code.replace(
        "#[derive(Debug, PartialEq)]\npub enum stable_how4",
        "#[derive(Debug, PartialEq, Clone, Copy)]\npub enum stable_how4",
    );
    let nfs4_code = nfs4_code.replace(
        "#[derive(Debug, PartialEq)]\npub enum nfs_ftype4",
        "#[derive(Debug, PartialEq, Clone, Copy)]\npub enum nfs_ftype4",
    );
    let nfs4_code = nfs4_code.replace(
        "#[derive(Debug, PartialEq)]\npub enum open_delegation_type4",
        "#[derive(Debug, PartialEq, Clone, Copy)]\npub enum open_delegation_type4",
    );
    let nfs4_code = nfs4_code.replace(
        "#[derive(Debug, PartialEq)]\npub enum state_protect_how4",
        "#[derive(Debug, PartialEq, Clone, Copy)]\npub enum state_protect_how4",
    );
    // fastxdr generates `try_variable_array::<u32>` for bitmap4 and ca_rdma_ird,
    // but u32 does not implement TryFrom<Bytes>. Replace with manual decoding.
    let nfs4_code = nfs4_code.replace(
        "v.try_variable_array::<u32>(None)?",
        "{ let n = v.try_u32()? as usize; let mut arr = Vec::with_capacity(n); for _ in 0..n { arr.push(v.try_u32()?); } arr }",
    );
    let nfs4_code = nfs4_code.replace(
        "v.try_variable_array::<u32>(Some(1))?",
        "{ let n = v.try_u32()? as usize; if n > 1 { return Err(Error::InvalidLength); } let mut arr = Vec::with_capacity(n); for _ in 0..n { arr.push(v.try_u32()?); } arr }",
    );
    fs::write(out_dir.join("nfs4_xdr.rs"), nfs4_code)?;

    println!("cargo:rerun-if-changed=src/nfs3/xdr/nfs.x");
    println!("cargo:rerun-if-changed=src/nfs3/xdr/mount.x");
    println!("cargo:rerun-if-changed=src/nfs41/xdr/nfs4.x");
    Ok(())
}
