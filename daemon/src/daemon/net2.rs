
use dsf_core::net::*;

/// Async network controller
/// Handles request and response routing for network messages
pub struct AsyncNet {
    tasks: UnboundedSender<(NetOp2, OneshotSender<NetRes>)>,
}

pub enum NetOp2 {
    Request(Vec<(Address, Option<Id>)>, Request),

    Exit,
}

pub enum NetRes {
    Responses(HashMap<Id, net::Response>),
}

impl AsyncNet {
    pub fn new() -> Self {
        // Setup control channel
        let (tx, mut rx) = unbounded_channel();

        // Setup network handler task
        // TODO: rework to perform all ops this way? or only writes?
        std::thread::spawn(move || {
            // Wait for store commands
            while let Some((op, done)) = rx.blocking_recv() {
                if let NetOp2::Exit = op {
                    debug!("Exiting AsyncNet");
                    break;
                }

                // Handle store operations
                let tx = match Self::handle_op(&mut store, op) {
                    Ok(r) => r,
                    Err(e) => {
                        error!("Failed to store object: {e:?}");
                        StoreRes::Err(e)
                    }
                };

                // Forward result back to waiting async tasks
                let _ = done.send(tx);
            }
        });
    }
}