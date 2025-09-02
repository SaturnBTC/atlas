pub use arch_program;

mod client;
mod helper;
mod types;

pub use client::*;
pub use helper::*;
pub use types::*;

/// Convenience macro for `AddAssign` with saturating arithmetic.
/// Replace by `std::num::Saturating` once stable
#[macro_export]
macro_rules! saturating_add_assign {
    ($i:expr, $v:expr) => {{
        $i = $i.saturating_add($v)
    }};
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_saturating_add_assign() {
        let mut i = 0u64;
        let v = 1;
        saturating_add_assign!(i, v);
        assert_eq!(i, 1);

        i = u64::MAX;
        saturating_add_assign!(i, v);
        assert_eq!(i, u64::MAX);
    }
}
