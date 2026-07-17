use super::*;

pub(crate) async fn record_flows_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let has_file = {
        let inner = state.inner.lock().await;
        inner.flows_file.is_some()
    };

    if !has_file {
        return next.run(request).await;
    }

    let method = request.method().to_string();
    let uri = request.uri();
    let path = uri
        .path_and_query()
        .map(|pq| pq.to_string())
        .unwrap_or_else(|| uri.path().to_string());
    let scheme = uri.scheme_str().unwrap_or("http").to_string();
    let host = request
        .headers()
        .get(header::HOST)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("localhost")
        .to_string();

    let mut request_headers = Vec::new();
    for (name, value) in request.headers() {
        if let Ok(val_str) = value.to_str() {
            request_headers.push(vec![name.to_string(), val_str.to_string()]);
        }
    }

    let ts_request = server_iso_now();
    let start_time = std::time::Instant::now();

    let (parts, body) = request.into_parts();
    let req_bytes = match to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(_) => Bytes::new(),
    };
    let request_body_b64 = BASE64_STANDARD.encode(&req_bytes);
    let request = Request::from_parts(parts, Body::from(req_bytes));

    let response = next.run(request).await;

    let duration_ms = start_time.elapsed().as_millis() as u64;
    let ts_response = server_iso_now();
    let status = response.status().as_u16();

    let mut response_headers = Vec::new();
    for (name, value) in response.headers() {
        if let Ok(val_str) = value.to_str() {
            response_headers.push(vec![name.to_string(), val_str.to_string()]);
        }
    }

    let (parts, body) = response.into_parts();
    let res_bytes = match to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(_) => Bytes::new(),
    };
    let response_body_b64 = BASE64_STANDARD.encode(&res_bytes);
    let response = Response::from_parts(parts, Body::from(res_bytes));

    let mut inner = state.inner.lock().await;
    let mut file_opt = inner.flows_file.take();
    if let Some(file) = &mut file_opt {
        inner.next_flow_index += 1;
        let flow_index = inner.next_flow_index;
        let flow_record = json!({
            "flow_index": flow_index,
            "ts_request": ts_request,
            "ts_response": ts_response,
            "duration_ms": duration_ms,
            "method": method,
            "scheme": scheme,
            "host": host,
            "path": path,
            "request_headers": request_headers,
            "request_body_b64": request_body_b64,
            "status": status,
            "response_headers": response_headers,
            "response_body_b64": response_body_b64,
        });
        if let Ok(line) = serde_json::to_string(&flow_record) {
            use std::io::Write;
            let _ = writeln!(file, "{}", line);
            let _ = file.flush();
        }
    }
    inner.flows_file = file_opt;

    response
}

pub(crate) fn server_iso_now() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    let millis = now.subsec_millis();

    let days = secs / 86400;
    let time_secs = secs % 86400;
    let hours = time_secs / 3600;
    let minutes = (time_secs % 3600) / 60;
    let seconds = time_secs % 60;

    let z = days as i64 + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    format!("{y:04}-{m:02}-{d:02}T{hours:02}:{minutes:02}:{seconds:02}.{millis:03}Z")
}
