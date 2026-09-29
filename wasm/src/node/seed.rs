use std::sync::Arc;

use ergot::{
    Address,
    interface_manager::{
        InterfaceState, Profile,
        profiles::{direct_edge::EDGE_NODE_ID, router::UPSTREAM_IDENT},
    },
    net_stack::services::{bridge_seed_assign, bridge_seed_refresh},
    time::{Duration, with_timeout},
    well_known::ErgotPingEndpoint,
};
use maitake_sync::WaitQueue;
use wasm_bindgen_futures::spawn_local;

use super::{RouterStack, closed_within};

/// Manage the seed lease of one pending bridge downlink: wait for the
/// bridge's uplink to become active, lease a network id from the upstream
/// seed router, warm the new net with one ping so the child learns its
/// address — then keep the lease alive by refreshing it before expiry
/// (leases start at 30 s; without refresh the upstream drops the route).
/// Falls back to a fresh assignment if refreshing fails repeatedly.
pub(super) fn spawn_seed_assign(stack: RouterStack, ident: u8, closer: Arc<WaitQueue>) {
    const FIRST_RETRY: Duration = Duration::from_millis(150);

    spawn_local(async move {
        'assign: loop {
            // Phase 1: wait for the uplink, then lease a net id.
            let mut retry = FIRST_RETRY;
            let lease = loop {
                if closed_within(&closer, retry).await {
                    return;
                }
                let upstream_active = stack.manage_profile(|im| {
                    matches!(
                        im.interface_state(UPSTREAM_IDENT),
                        Some(InterfaceState::Active { .. })
                    )
                });
                if !upstream_active {
                    retry = FIRST_RETRY;
                    continue;
                }
                match bridge_seed_assign(&stack, UPSTREAM_IDENT, ident).await {
                    Ok(lease) => break lease,
                    Err(e) => {
                        retry = (retry * 2).min(Duration::from_secs(5));
                        log::warn!("seed assignment failed (retry in {retry:?}): {e:?}");
                    }
                }
            };

            // Warm the leased net so the child learns its address.
            let addr = Address {
                network_id: lease.net_id,
                node_id: EDGE_NODE_ID,
                port_id: 0,
            };
            let warm = async {
                let _ = stack
                    .endpoints()
                    .request::<ErgotPingEndpoint>(addr, &0u32, Some("ping"))
                    .await;
            };
            let _ = with_timeout(Duration::from_millis(300), warm).await;

            // Phase 2: keep the lease alive.
            let mut lease = lease;
            let mut failures = 0u32;
            loop {
                let delay = if failures == 0 {
                    Duration::from_secs(
                        u64::from(
                            lease
                                .expires_seconds
                                .saturating_sub(lease.min_refresh_seconds),
                        )
                        .max(1),
                    )
                } else {
                    Duration::from_secs(2) // retry quickly while the lease is still at risk
                };
                if closed_within(&closer, delay).await {
                    return;
                }
                match bridge_seed_refresh(&stack, &lease).await {
                    Ok(refreshed) => {
                        lease = refreshed;
                        failures = 0;
                    }
                    Err(e) => {
                        failures += 1;
                        log::warn!("seed refresh failed ({failures}x): {e:?}");
                        if failures >= 3 {
                            // The lease is likely gone upstream; start over.
                            continue 'assign;
                        }
                    }
                }
            }
        }
    });
}
