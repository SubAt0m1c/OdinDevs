use std::{borrow::Borrow, fmt::Display, ops::Deref, sync::Arc};

use redb::TypeName;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct ArcStr(Arc<str>);

impl Serialize for ArcStr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'a> Deserialize<'a> for ArcStr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'a>,
    {
        let s: &'a str = serde::Deserialize::deserialize(deserializer)?;
        Ok(Self(Arc::from(s)))
    }
}

impl ArcStr {
    pub fn new(s: &str) -> Self {
        Self(Arc::from(s))
    }

    pub fn from_string(s: String) -> Self {
        Self(Arc::from(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for ArcStr {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<str> for ArcStr {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Deref for ArcStr {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl Display for ArcStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_str().fmt(f)
    }
}

// ------------------------------------------------------ RedB impls ----------------------------------------

impl redb::Key for ArcStr {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        data1.cmp(data2)
    }
}

impl redb::Value for ArcStr {
    type SelfType<'a> = Self;
    type AsBytes<'a> = &'a [u8];

    fn fixed_width() -> Option<usize> {
        None
    }
    
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b
    {
        value.as_bytes()
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a
    {
        let str = str::from_utf8(data).expect("'data' should be utf8!");
        ArcStr::new(str)
    }

    fn type_name() -> redb::TypeName {
        TypeName::new("odindevs::arc_str")
    }
}