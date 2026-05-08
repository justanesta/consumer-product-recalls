# NHTSA cassette inventory

This directory's cassette pattern **deviates from CPSC, FDA, and USDA cassettes
intentionally.** Read this before recording or re-recording.

## What's different about NHTSA cassettes

CPSC, FDA, and USDA cassettes commit **real live response bodies** because the
response body shape *is* the schema being archived. CPSC cassettes are 13–15 MB
each; USDA cassettes are 14 MB.

A NHTSA POST_2010 cassette with the real live response body would be ~20 MB YAML
(~14 MB binary base64-encoded), and a deep-rescan cassette would add another
~10 MB. Committing 30+ MB of binary-encoded ZIP body to git is wasteful when
the **continuous schema archive role is already fulfilled by the watermark probe**:

- `scripts/nhtsa/probe_watermarks.sh` runs daily, captures HEAD + body SHA-256
  for every NHTSA URL (15 URLs daily vs. 1 URL × point-in-time for a cassette)
- `documentation/nhtsa/watermark_probes.jsonl` is the committed archive
- Findings A, B, C in `documentation/nhtsa/flat_file_observations.md` are
  closed because of this probe data

So NHTSA cassettes here serve a **narrower purpose**: archive the real S3
response **headers** (ETag, Last-Modified, x-amz-version-id, content-type,
cache-control, x-amz-replication-status) so test-time replay exercises the real
httpx → response → header-handling path. The body is replaced with the small
1.8 KB `tests/fixtures/nhtsa/sample_recalls.zip` fixture (10 hand-built rows).
The extractor doesn't depend on production body content for correctness; it
just needs a valid TSV-ZIP to decompress.

## Recording / refreshing the cassette

One-time setup or refresh (when first creating, or when NHTSA's HTTP envelope
changes — typically caught first by the watermark probe):

```bash
# 1. Live-record against real NHTSA. Produces ~14 MB YAML.
pytest --vcr-record=all tests/integration/test_nhtsa_live_cassettes.py \
    -k test_happy_path_full_dump

# 2. Hand-edit the resulting test_happy_path_full_dump.yaml:
#    - Read tests/fixtures/nhtsa/sample_recalls.zip → bytes
#    - Base64-encode (cassette body in YAML is base64)
#    - Replace the `body.string:` payload with the encoded fixture
#    - Update the `Content-Length:` response header to match the new body
#      byte count (1821 bytes for the current fixture)
#    - Save; resulting cassette ~5–10 KB
#
# 3. Verify the test still passes against the swapped cassette:
pytest tests/integration/test_nhtsa_live_cassettes.py
```

## ADR linkage

- ADR 0014 — schema-evolution workflow (cassettes are an archive role here)
- ADR 0015 — testing strategy (VCR + respx layering)
- ADR 0031 — silver-row fragmentation strategy (frames the watermark probe as
  NHTSA's continuous schema-evolution archive, complementary to this cassette)
