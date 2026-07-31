#!/usr/bin/env bash
set -euo pipefail

# The current kernel-nfsd lab has no supported control for originating a callback
# and dropping only its reply. Run the deterministic reply-loss/retransmission
# scenario explicitly in nightly while capability-report.sh records that real
# server injection remains unavailable.
cargo test --locked \
  nfs41::callback::tests::scripted_callback_reply_loss_replays_cached_body_and_executes_recall_once \
  -- --exact --nocapture
