//! Smart HTTP request handling that a mirror needs *before* it serves a pack:
//! decoding the body Git sent and finding out which objects the client asked
//! for, so a miss can be filled from the upstream instead of answered with
//! `not our ref`.

use std::path::Path;

use crate::MirrorError;

/// Decode a Git request body, gunzipping when the client compressed it.
///
/// Git gzip-encodes large POST bodies (observed on full-history fetches:
/// thousands of want lines, ~6 KB). `git http-backend` expects decoded
/// pkt-lines on stdin — a real front-end gunzips first — so without this the
/// backend dies with `bad line length character` and the client reports
/// `fatal: expected 'packfile'`. Small bodies (ls-refs, single-want fetches)
/// arrive plain and pass through untouched.
///
/// `max_bytes` bounds the *decoded* size: Git already caps the compressed wire
/// body, and without a second cap a tiny gzip expands past the same budget in
/// RAM.
pub fn decode_body(
    body: &[u8],
    content_encoding: Option<&str>,
    max_bytes: usize,
) -> Result<Vec<u8>, MirrorError> {
    let gzipped = content_encoding
        .unwrap_or_default()
        .split(',')
        .any(|value| value.trim().eq_ignore_ascii_case("gzip"));
    if !gzipped {
        return Ok(body.to_vec());
    }
    use flate2::read::GzDecoder;
    use std::io::Read as _;
    let mut decoded = Vec::new();
    let mut decoder = GzDecoder::new(body).take(max_bytes as u64 + 1);
    decoder.read_to_end(&mut decoded).map_err(|error| {
        MirrorError::InvalidInput(format!("invalid gzip Git request body: {error}"))
    })?;
    if decoded.len() > max_bytes {
        return Err(MirrorError::TooLarge(
            "gzip Git request body exceeded size limit".to_owned(),
        ));
    }
    Ok(decoded)
}

/// Every full object id a `git-upload-pack` request asks for.
///
/// Both wire shapes occur: protocol v0/v1 starts with `want <oid>` lines and
/// protocol v2 sends `command=fetch` followed by the same want lines inside
/// the request section. Parsing pkt-lines and looking for the `want` verb
/// covers both without negotiating a capability set first.
///
/// Ids are returned sorted and deduplicated so a caller's presence checks are
/// not repeated for the same object, and only full 40–64 hex ids appear —
/// a symbolic want (`want main`) names a ref the mirror is meant to serve
/// from its own advertisement, never something to fetch by name.
pub fn wanted_object_ids(body: &[u8]) -> Vec<String> {
    let mut wanted = Vec::new();
    let mut offset = 0;
    while offset + 4 <= body.len() {
        let Ok(length_text) = std::str::from_utf8(&body[offset..offset + 4]) else {
            break;
        };
        let Ok(length) = usize::from_str_radix(length_text, 16) else {
            break;
        };
        offset += 4;
        // flush-pkt (0000), delim-pkt (0001) and response-end (0002) carry no
        // payload but do advance the stream.
        if length < 4 {
            continue;
        }
        let payload_end = offset + (length - 4);
        if payload_end > body.len() {
            break;
        }
        let payload = &body[offset..payload_end];
        offset = payload_end;
        // A want line may be followed by capabilities after a NUL or newline,
        // and separated from them by spaces.
        let line = payload
            .split(|byte| *byte == 0 || *byte == b'\n')
            .next()
            .unwrap_or(payload);
        let Some(rest) = line.strip_prefix(b"want ") else {
            continue;
        };
        let Ok(text) = std::str::from_utf8(rest) else {
            continue;
        };
        let oid = text.split(' ').next().unwrap_or(text).trim();
        if crate::is_full_object_id(oid) {
            wanted.push(oid.to_ascii_lowercase());
        }
    }
    wanted.sort();
    wanted.dedup();
    wanted
}

/// Fetch the wanted objects the mirror is missing, under the repository lock.
///
/// This is the fetch-on-miss path: a client that asks for a commit the mirror
/// has never seen gets it fetched from `upstream` and saved, instead of the
/// backend failing the negotiation. Presence is checked twice — once before
/// taking the lock, so a cache hit costs no lock at all, and once after, so
/// concurrent requests for the same commit fetch it once rather than twice.
///
/// Returns the ids this call actually fetched (empty when everything was
/// already present). Credentials are the caller's: the mirror crate knows
/// nothing about which forge a repository lives on.
pub async fn fetch_missing_wanted(
    repository: &Path,
    lock_path: &Path,
    wanted: &[String],
    upstream: &str,
    auth_header: Option<(&str, &str)>,
    deadline: std::time::Duration,
) -> Result<Vec<String>, MirrorError> {
    let mut missing = Vec::new();
    for oid in wanted {
        if !crate::cache::cache_holds_commit(repository, oid).await {
            missing.push(oid.clone());
        }
    }
    if missing.is_empty() {
        return Ok(Vec::new());
    }
    let _guard = crate::lock::acquire(lock_path).await?;
    let mut fetched = Vec::new();
    for oid in missing {
        if crate::cache::cache_holds_commit(repository, &oid).await {
            // A sibling request filled it while this one waited for the lock.
            continue;
        }
        if crate::cache::fetch_commit(repository, upstream, &oid, auth_header, deadline)
            .await?
            .fetched_from_upstream()
        {
            fetched.push(oid);
        }
    }
    Ok(fetched)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encode one pkt-line.
    fn pkt(payload: &[u8]) -> Vec<u8> {
        let mut line = format!("{:04x}", payload.len() + 4).into_bytes();
        line.extend_from_slice(payload);
        line
    }

    #[test]
    fn wants_are_read_from_protocol_zero_and_two() {
        let oid = "a".repeat(40);
        let other = "B".repeat(40);
        let mut body = pkt(b"want aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa agent=git/2.43\n");
        body.extend_from_slice(&pkt(format!("want {other}").as_bytes()));
        body.extend_from_slice(b"0000");
        body.extend_from_slice(&pkt(b"deepen 1"));
        assert_eq!(
            wanted_object_ids(&body),
            vec![oid.clone(), other.to_ascii_lowercase()]
        );

        let mut v2 = pkt(b"command=fetch\n");
        v2.extend_from_slice(&pkt(b"agent=git/2.43\n"));
        v2.push(b'0');
        v2.push(b'0');
        v2.push(b'0');
        v2.push(b'1');
        v2.extend_from_slice(&pkt(format!("want {oid}\n").as_bytes()));
        v2.extend_from_slice(b"0000");
        assert_eq!(wanted_object_ids(&v2), vec![oid]);
    }

    #[test]
    fn symbolic_wants_and_truncated_bodies_are_ignored() {
        let mut body = pkt(b"want main\n");
        body.extend_from_slice(&pkt(b"want --upload-pack=evil\n"));
        assert!(wanted_object_ids(&body).is_empty());
        // A truncated final pkt is not parsed into a bogus id.
        let mut truncated = pkt(&format!("want {}\n", "c".repeat(40)).into_bytes());
        truncated.truncate(truncated.len() - 3);
        assert!(wanted_object_ids(&truncated).is_empty());
    }

    #[test]
    fn gzip_bodies_are_decoded_and_bounded() {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use std::io::Write as _;
        let plain = pkt(&format!("want {}\n", "d".repeat(40)).into_bytes());
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&plain).unwrap();
        let compressed = encoder.finish().unwrap();

        let decoded = decode_body(&compressed, Some("gzip"), 1 << 20).unwrap();
        assert_eq!(decoded, plain);
        // Uncompressed bodies pass through even when the header is absent.
        assert_eq!(decode_body(&plain, None, 1 << 20).unwrap(), plain);
        // The decoded size, not the wire size, is what the cap bounds.
        assert!(decode_body(&compressed, Some("gzip"), 8).is_err());
    }

    #[tokio::test]
    async fn a_missing_want_is_fetched_once_and_then_served_from_cache() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        std::fs::create_dir_all(&source).unwrap();
        let run = |args: &[&str]| {
            let status = std::process::Command::new("git")
                .arg("-C")
                .arg(&source)
                .args(args)
                .status()
                .unwrap();
            assert!(status.success());
        };
        run(&["init", "-q", "-b", "main"]);
        std::fs::write(source.join("a.txt"), "a").unwrap();
        run(&["add", "a.txt"]);
        run(&[
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@example.com",
            "commit",
            "-q",
            "-m",
            "a",
        ]);
        let sha = String::from_utf8(
            std::process::Command::new("git")
                .arg("-C")
                .arg(&source)
                .args(["rev-parse", "HEAD"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .to_owned();

        let mirror = temp.path().join("mirror.git");
        crate::cache::ensure_bare_repository(&mirror).await.unwrap();
        let lock_path = temp.path().join("mirror.lock");
        let upstream = format!("file://{}", source.display());

        let fetched = fetch_missing_wanted(
            &mirror,
            &lock_path,
            std::slice::from_ref(&sha),
            &upstream,
            None,
            crate::GIT_CONTROL_TIMEOUT,
        )
        .await
        .unwrap();
        assert_eq!(fetched, vec![sha.clone()]);

        // Second call: present, so no lock is taken and nothing is fetched.
        let again = fetch_missing_wanted(
            &mirror,
            &lock_path,
            std::slice::from_ref(&sha),
            &upstream,
            None,
            crate::GIT_CONTROL_TIMEOUT,
        )
        .await
        .unwrap();
        assert!(again.is_empty());
        assert!(!lock_path.exists());
    }
}
