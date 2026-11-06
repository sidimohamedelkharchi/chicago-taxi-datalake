from pipeline.ingestion import soda

HEADER = '"trip_id","fare"\n'


class FakeResponse:
    def __init__(self, text):
        self.content = text.encode()

    def raise_for_status(self):
        pass


class FakeSession:
    """Serves 5 rows with a page size of 2 -> pages of 2, 2, 1."""

    def __init__(self):
        self.calls = []

    def get(self, url, params, timeout):
        self.calls.append(params)
        offset, limit = int(params["$offset"]), int(params["$limit"])
        rows = [f'"id{i}","1.0"\n' for i in range(5)][offset : offset + limit]
        return FakeResponse(HEADER + "".join(rows))


def test_month_helpers():
    assert soda.next_month("2023-12") == "2024-01"
    assert soda.month_range("2023-11", "2024-02") == ["2023-11", "2023-12", "2024-01"]


def test_build_params_filters_one_month():
    params = soda.build_params("2023-03", 100, 200)
    assert params["$where"] == (
        "trip_start_timestamp >= '2023-03-01T00:00:00' AND trip_start_timestamp < '2023-04-01T00:00:00'"
    )
    assert params["$order"] == "trip_id" and params["$offset"] == "200"


def test_pagination_stops_on_short_page():
    session = FakeSession()
    pages = list(soda.iter_api_pages("2023-01", page_size=2, session=session, base_url="http://x"))
    assert [p.rows for p in pages] == [2, 2, 1]
    assert len(session.calls) == 3


def test_count_rows_handles_quoted_newlines():
    assert soda.count_csv_rows(b'"a","b"\n"1","multi\nline"\n"2","x"\n') == 2


def test_local_cache(tmp_path):
    folder = tmp_path / "trip_month=2023-01"
    folder.mkdir()
    (folder / "page_00000.csv").write_text(HEADER + '"id1","2"\n')
    assert soda.has_local_cache("2023-01", str(tmp_path))
    assert not soda.has_local_cache("2023-02", str(tmp_path))
    assert [p.rows for p in soda.iter_local_pages("2023-01", str(tmp_path))] == [1]
