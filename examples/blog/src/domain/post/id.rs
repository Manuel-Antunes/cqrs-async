use std::fmt;

use serde::{Deserialize, Serialize};

/// Identifies a post. Its `Display` form (`42`) is the suffix of the post's
/// event stream (`post-42`), so it must never change for a stored post.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PostId(pub u64);

impl fmt::Display for PostId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Back from the `Display` form (a stream's suffix).
impl std::str::FromStr for PostId {
    type Err = std::num::ParseIntError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        text.parse().map(Self)
    }
}
