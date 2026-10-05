SET search_path = control;
CREATE INDEX jobs_ready_global ON jobs(priority DESC, run_order, job_order, run_id, job_id)
    WHERE queue_state = 'ready';
