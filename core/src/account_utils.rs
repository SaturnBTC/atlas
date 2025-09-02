use arch_program::{account::AccountMeta, pubkey::Pubkey};

pub fn next_account<'a>(iter: &mut impl Iterator<Item = &'a AccountMeta>) -> Option<Pubkey> {
    Some(iter.next()?.pubkey)
}
