bitflags! {
    /// Page and Message Flags.
    #[derive(Default)]
    #[cfg_attr(feature = "defmt", derive(defmt::Format))]
    #[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
    pub struct Flags: u16 {
        /// Secondary page flag
        const SECONDARY       = (1 << 0);

        /// Tertiary page flag
        const TERTIARY        = (1 << 1);

        /// Signal an object is encrypted
        const ENCRYPTED       = (1 << 2);

        /// Specify service is of nameservice type (primary page only)
        const NAMESERVICE     = (1 << 3);

        /// Request that the response contains a service address (messages only)
        const ADDRESS_REQUEST = (1 << 3);
        /// Request that the response contains a public key (messages only)
        const PUB_KEY_REQUEST = (1 << 4);

        /// Signal symmetric encryption is enabled (messages only)
        const SYMMETRIC_MODE = (1 << 5);
        /// Set direction flag for symmetric encryption (messages only)
        const SYMMETRIC_DIR  = (1 << 6);

        /// Signal a device is constrained (requests are delegation, not for use as DHT peer)
        const CONSTRAINED = (1 << 7);

        /// Signal a device is transient, limiting persistence of ID and address
        const NO_PERSIST = (1 << 8);

        /// (subscribe request) prioritise latency, eliding message containers (and thus p2p encryption)
        const QOS_PRIO_LATENCY = (1 << 9);
    }
}
