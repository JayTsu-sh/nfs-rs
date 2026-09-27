use std::env;
use std::fs;
use std::path::PathBuf;

fn disambiguate_empty_byte_slices(code: String) -> String {
    code.replace("assert_eq!(buf.as_ref(), &[]);", "assert!(buf.is_empty());")
}

/// fastxdr 1.0.2's decoder header checks that an opaque's data (`try_bytes`) or an array's
/// elements (`try_variable_array`) are in the buffer, then advances past the XDR padding that
/// follows them unchecked. A reply that ends inside that padding (RFC 4506 §4.10: 0 to 3 zero
/// bytes after a name, handle or other opaque) makes `Bytes::advance` panic instead of failing
/// the decode. Check the padding too, so a truncated or malformed reply is
/// `Error::InvalidLength` (surfaced as `NfsError::Xdr`), never a panic.
///
/// The build fails if the generated header no longer holds exactly one of each unchecked
/// advance, so an upgraded generator is looked at again rather than silently unpatched.
fn check_padding(code: String, file: &str) -> Result<String, String> {
    const PATCHES: [(&str, &str); 2] = [
        (
            "self.advance(n + pad_length(n));",
            "if self.remaining() - n < pad_length(n) { return Err(Error::InvalidLength); }\n            self.advance(n + pad_length(n));",
        ),
        (
            "self.advance(pad_length(sum));",
            "if self.remaining() < pad_length(sum) { return Err(Error::InvalidLength); }\n            self.advance(pad_length(sum));",
        ),
    ];
    let mut code = code;
    for (unchecked, checked) in PATCHES {
        let found = code.matches(unchecked).count();
        if found != 1 {
            return Err(format!(
                "{file}: expected one `{unchecked}` in the fastxdr header, found {found}"
            ));
        }
        code = code.replacen(unchecked, checked, 1);
    }
    Ok(code)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);

    let nfs_xdr = fs::read_to_string("src/nfs3/xdr/nfs.x")?;
    let nfs_code = fastxdr::Generator::default().generate(&nfs_xdr)?;
    // fastxdr wraps generated code in `mod xdr { ... }` (private). Make it public
    // so it can be re-exported from the parent module.
    let nfs_code = nfs_code.replacen("mod xdr {", "pub mod xdr {", 1);
    // fastxdr generates bool-typed discriminants matched against integer literals,
    // which is invalid in Rust 2021. Replace with bool literals.
    let nfs_code = nfs_code.replace("1 => Self::TRUE(", "true => Self::TRUE(");
    let nfs_code = nfs_code.replace("0 => Self::FALSE,", "false => Self::FALSE,");
    fs::write(
        out_dir.join("nfs_xdr.rs"),
        check_padding(disambiguate_empty_byte_slices(nfs_code), "nfs_xdr.rs")?,
    )?;

    let mount_xdr = fs::read_to_string("src/nfs3/xdr/mount.x")?;
    let mount_code = fastxdr::Generator::default().generate(&mount_xdr)?;
    let mount_code = mount_code.replacen("mod xdr {", "pub mod xdr {", 1);
    // fastxdr generates `try_variable_array::<i32>` for auth_flavors, but i32 does not
    // implement TryFrom<Bytes>. Replace with manual per-element decoding.
    let mount_code = mount_code.replace(
        "auth_flavors: v.try_variable_array::<i32>(None)?",
        "auth_flavors: { let n = v.try_u32()? as usize; let mut arr = Vec::with_capacity(n); for _ in 0..n { arr.push(v.try_i32()?); } arr }",
    );
    fs::write(
        out_dir.join("mount_xdr.rs"),
        check_padding(disambiguate_empty_byte_slices(mount_code), "mount_xdr.rs")?,
    )?;

    // NFSv4 common XDR types plus explicit NFSv4.1 extensions.
    let nfs4_xdr = fs::read_to_string("src/nfs4/xdr/nfs4.x")?;
    let nfs4_code = fastxdr::Generator::default().generate(&nfs4_xdr)?;
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
    fs::write(
        out_dir.join("nfs4_xdr.rs"),
        check_padding(disambiguate_empty_byte_slices(nfs4_code), "nfs4_xdr.rs")?,
    )?;

    println!("cargo:rerun-if-changed=src/nfs3/xdr/nfs.x");
    println!("cargo:rerun-if-changed=src/nfs3/xdr/mount.x");
    println!("cargo:rerun-if-changed=src/nfs4/xdr/nfs4.x");
    Ok(())
}
