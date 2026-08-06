// repro/test_bugMC-S5-step-transition-loss_queue.rs
// Demonstrates MC-S5: terminal step transition lost on failed POST.
// Level 2: reachable state injection via queue_update + simulated publish fail.
// Matches TLC: WorkerQueueUpdate (terminal) -> WorkerTakeBody (nonempty dirty) -> publish=false -> next TakeBody empty.
// Real consumer: backend steps updater never sees terminal status (reporting.rs:57).
// Runs with: cargo test --test test_bugMC-S5-step-transition-loss_queue --lib

#[path = "../worktree/crates/aksh-runner/src/worker/server_queue.rs"]
mod server_queue;
use server_queue::{ServerQueue, StepUpdate, step_status, step_conclusion};

#[test]
fn reproduces_step_transition_loss_on_failed_post() {
    let mut q = ServerQueue::new("job1".into(), "plan1".into());
    
    // Terminal step update (as in MC trace for Scenario 5 terminal)
    q.queue_update(StepUpdate {
        external_id: "st1".into(),
        number: 1,
        name: "terminal-step".into(),
        status: step_status::COMPLETED,
        started_at: Some("2024-01-01T00:00:00Z".into()),
        completed_at: Some("2024-01-01T00:00:01Z".into()),
        conclusion: step_conclusion::SUCCEEDED,
    });
    
    // First take (POST sent, dirty cleared inside)
    let (body1, gen1) = q.take_steps_update_body().expect("first body");
    assert_eq!(body1.steps.len(), 1, "first take sends terminal step");
    assert_eq!(body1.steps[0].conclusion, step_conclusion::SUCCEEDED);
    
    // Simulate failed POST: do NOT call mark_steps_published(gen1)
    // This leaves steps_generation > published_generation but dirty cleared
    
    // Second take (next flush): should lose the transition per bug
    let pending2 = q.take_steps_update_body();
    assert!(pending2.is_some(), "gen mismatch still triggers take");
    let (body2, _gen2) = pending2.unwrap();
    assert_eq!(body2.steps.len(), 0, "BUG REPRODUCED: empty body, terminal step transition LOST");
    
    // has_pending still true due to gen, but no content sent
    assert!(q.has_pending(), "state remains pending but lost");
    
    println!("REPRODUCED: MC-S5-step-transition-loss confirmed. Terminal transition lost on failed POST.");
    println!("Checklist:");
    println!("1. Level 0/1? no (needs failure on terminal POST)");
    println!("2. Reachable: exact CE step WorkerTakeBody after terminal WorkerQueueUpdate (gen != pubGen, dirty non-empty -> fail -> empty dirty)");
    println!("3. Real consumer: update_workflow_steps / AzDO timeline updater (reporting.rs:55-72) observes missing terminal status.");
    println!("4. Permanent: no resend/loopback; published_generation advances past lost transition.");
}

fn main() {
    reproduces_step_transition_loss_on_failed_post();
}