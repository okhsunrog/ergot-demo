use std::sync::Arc;

use embassy_futures::select::{Either, select};
use ergot::{
    Address,
    interface_manager::{
        InterfaceState, Profile,
        profiles::{direct_edge::EDGE_NODE_ID, router::UPSTREAM_IDENT},
    },
    net_stack::services::bridge_seed_assign,
    well_known::ErgotPingEndpoint,
};
use gloo_timers::future::TimeoutFuture;
use maitake_sync::WaitQueue;
use wasm_bindgen_futures::spawn_local;

use super::RouterStack;

/// Manage the seed lease of one pending bridge downlink: wait for the
/// bridge's uplink to become active, lease a network id from the upstream
/// seed router, warm the new net with one ping so the child learns its
/// address — then keep the lease alive by refreshing it before expiry
/// (leases start at 30 s; without refresh the upstream drops the route).
/// Falls back to a fresh assignment if refreshing fails repeatedly.
pub(super) fn spawn_seed_assign(stack: RouterStack, ident: u8, closer: Arc<WaitQueue>) {
    /// Wait `ms`; true means the link closed and the task should end.
    async fn closed_within(closer: &WaitQueue, ms: u32) -> bool {
        matches!(
            select(TimeoutFuture::new(ms), closer.wait()).await,
            Either::Second(_)
        )
    }

    spawn_local(async move {
        'assign: loop {
            // Phase 1: wait for the uplink, then lease a net id.
            let mut retry_ms = 150;
            let lease = loop {
                if closed_within(&closer, retry_ms).await {
                    return;
                }
                let upstream_active = stack.manage_profile(|im| {
                    matches!(
                        im.interface_state(UPSTREAM_IDENT),
                        Some(InterfaceState::Active { .. })
                    )
                });
                if !upstream_active {
                    retry_ms = 150;
                    continue;
                }
                match bridge_seed_assign(&stack, UPSTREAM_IDENT, ident).await {
                    Ok(lease) => break lease,
                    Err(e) => {
                        retry_ms = (retry_ms * 2).min(5_000);
                        log::warn!("seed assignment failed (retry in {retry_ms} ms): {e:?}");
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
            let _ = select(warm, TimeoutFuture::new(300)).await;

            // Phase 2: keep the lease alive.
            let mut lease = lease;
            let mut failures = 0u32;
            loop {
                let delay_s = if failures == 0 {
                    u32::from(
                        lease
                            .expires_seconds
                            .saturating_sub(lease.min_refresh_seconds),
                    )
                    .max(1)
                } else {
                    2 // retry quickly while the lease is still at risk
                };
                if closed_within(&closer, delay_s * 1000).await {
                    return;
                }
                match ergot::net_stack::services::bridge_seed_refresh(&stack, &lease).await {
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
