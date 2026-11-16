"""Finance's budget drop: workbooks uploaded to a Cloud Storage bucket, re-uploaded when corrected."""

from __future__ import annotations

from vendorsim import data


def gcs_budget_sink(bucket_name: str):
    from storeops.landing import gcs_client

    client = gcs_client()
    bucket = client.bucket(bucket_name)
    if not bucket.exists():
        bucket = client.create_bucket(bucket_name)

    def sink(name: str, month: str, version: int) -> None:
        blob = bucket.get_blob(name)
        if blob is not None and int((blob.metadata or {}).get("version", "0")) >= version:
            return
        new = bucket.blob(name)
        new.metadata = {"version": str(version)}
        new.upload_from_string(data.budget_workbook(month, version),
                               content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    return sink
