#[repr(transparent)]
#[derive(Clone, Copy)]
pub(crate) struct BvhCode(u32);

#[derive(Clone, Copy)]
pub(crate) enum BvhNode {
    Leaf(usize),
    Internal(usize),
}

impl BvhCode {
    pub(crate) const LEAF_BIT: u32 = 1 << 31;

    pub(crate) const fn new(raw: u32) -> Self {
        Self(raw)
    }

    pub(crate) const fn leaf(index: usize) -> Self {
        Self(Self::LEAF_BIT | index as u32)
    }

    pub(crate) const fn internal(index: u32) -> Self {
        Self(index)
    }

    pub(crate) const fn raw(self) -> u32 {
        self.0
    }

    pub(crate) const fn decode(self) -> BvhNode {
        if self.0 & Self::LEAF_BIT != 0 {
            BvhNode::Leaf((self.0 & !Self::LEAF_BIT) as usize)
        } else {
            BvhNode::Internal(self.0 as usize)
        }
    }
}
