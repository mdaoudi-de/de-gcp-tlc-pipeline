import requests

from de_pipeline.ingestion.downloader import download_file


class DummyResponse:
    def __init__(self, content_chunks):
        self._chunks = content_chunks

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size=1):
        yield from self._chunks


def test_download_file_writes_bytes(tmp_path, monkeypatch):
    chunks = [b"abc", b"def"]

    def fake_get(url, stream=True, timeout=60):
        return DummyResponse(chunks)

    monkeypatch.setattr(requests, "get", fake_get)

    dest = tmp_path / "out.bin"
    meta = download_file("http://example.com/file", dest, timeout_sec=60, chunk_size=2)

    assert dest.exists()
    assert dest.read_bytes() == b"abcdef"
    assert meta["bytes"] == 6
    assert "md5" in meta
