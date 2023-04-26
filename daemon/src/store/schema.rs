use diesel::prelude::*;
use diesel::*;

table! {
    services (service_id) {
        service_id -> Text,
        service_index -> Integer,

        state -> Text,

        public_key -> Text,
        private_key -> Nullable<Text>,
        secret_key -> Nullable<Text>,

        primary_page -> Nullable<Text>,
        replica_page -> Nullable<Text>,

        last_updated -> Nullable<Timestamp>,

        subscribers -> Integer,
        replicas -> Integer,
        original -> Bool,
        subscribed -> Bool,
    }
}

table! {
    peers (peer_id) {
        peer_id -> Text,
        peer_index -> Integer,
        state -> Text,
        public_key -> Nullable<Text>,

        address -> Text,
        address_mode -> Text,

        last_seen -> Nullable<Timestamp>,

        sent -> Integer,
        received -> Integer,
        blocked -> Bool,
    }
}

table! {
    peer_addresses (peer_id) {
        peer_id -> Text,

        address -> Text,
        address_mode -> Text,

        last_used -> Nullable<Timestamp>,
    }
}

table! {
    subscriptions (service_id, peer_id) {
        service_id -> Text,
        peer_id -> Text,

        last_updated -> Nullable<Timestamp>,
        expiry -> Nullable<Timestamp>,
    }
}

table! {
    subscribers (service_id, peer_id) {
        service_id -> Text,
        peer_id -> Text,

        last_updated -> Nullable<Timestamp>,
        expiry -> Nullable<Timestamp>,
    }
}

table! {
    data (signature) {
        service_id -> Text,
        object_index -> Integer,

        body_kind -> Text,
        body_value -> Nullable<Blob>,

        previous -> Nullable<Text>,
        signature -> Text,
    }
}

table! {
    object (signature) {
        service_id -> Text,

        raw_data -> Blob,

        previous -> Nullable<Text>,
        signature -> Text,
    }
}

table! {
    identity (service_id) {
        service_id -> Text,

        public_key -> Text,
        private_key -> Text,
        secret_key -> Nullable<Text>,

        last_page -> Text,
    }
}
