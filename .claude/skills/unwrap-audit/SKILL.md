---
name: unwrap-audit
description: Scan all production Rust code for unwrap()/expect() violations per CLAUDE.md rules
---

Scan all `.rs` files under `src/` for `.unwrap()` and `.expect(` calls in production code.

Exclude:
- `#[cfg(test)]` blocks and `mod tests` sections
- `src/bindings.rs` (auto-generated)

Run:
```bash
grep -rn '\.unwrap()\|\.expect(' --include="*.rs" src/ | grep -v 'bindings\.rs' | grep -v '#\[cfg(test)\]' | grep -v 'mod tests'
```

For each finding:
1. Report file path and line number
2. Show the line of code
3. Classify: is this truly dangerous or safe (e.g., `unwrap_or_default` is fine)?
4. Suggest the specific replacement: `?`, `map_err`, `if let Ok`, `unwrap_or_default`, `match`, etc.

Group results by file. End with a count summary.
