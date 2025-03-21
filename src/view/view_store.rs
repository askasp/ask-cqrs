/// Wait for a view to catch up to a specific position
pub async fn wait_for_view<T: ViewState>(
    &self,
    stream_name: &str,
    stream_id: &str,
    instance_id: &str,
    min_position: i64,
    timeout_ms: i64,
) -> Result<bool, Error> {
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_millis(timeout_ms as u64);
    
    loop {
        if start.elapsed() > timeout {
            return Err(Error::Timeout(format!(
                "Timeout waiting for view {} to catch up to position {} for stream {}/{}",
                instance_id, min_position, stream_name, stream_id
            )));
        }

        let state = self.get_view_state_position::<T>(instance_id, stream_name, stream_id).await?;
        
        tracing::debug!(
            "Waiting for view {} to catch up: current_position={:?}, min_position={}",
            instance_id, state, min_position
        );
        
        match state {
            Some(position) if position >= min_position => {
                tracing::debug!(
                    "View {} caught up to position {} for stream {}/{}",
                    instance_id, position, stream_name, stream_id
                );
                return Ok(true);
            }
            _ => {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }
    }
}

/// Wait for a view to catch up to a specific event
pub async fn wait_for_view_by_event<T: ViewState>(
    &self,
    event: &EventRow,
    instance_id: &str,
    timeout_ms: i64,
) -> Result<bool, Error> {
    self.wait_for_view::<T>(
        &event.stream_name,
        &event.stream_id,
        instance_id,
        event.stream_position,
        timeout_ms
    ).await
} 