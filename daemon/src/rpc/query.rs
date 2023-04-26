use crate::daemon::{net::NetIf, Dsf};

impl<Net> Dsf<Net> where Dsf<Net>: NetIf<Interface = Net> {}
