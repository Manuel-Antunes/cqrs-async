//! The macros' error messages, as a user sees them (`trybuild`). Compiler
//! diagnostics change between Rust versions, so it runs on stable only:
//! `cargo test -p cqrs-async --test ui -- --ignored`; `TRYBUILD=overwrite` to
//! accept new messages after reading them.

#[test]
#[ignore = "diagnostics depend on the compiler version: run on stable with --ignored"]
fn macro_errors() {
    trybuild::TestCases::new().compile_fail("tests/ui/*.rs");
}
