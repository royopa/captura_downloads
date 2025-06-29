from datetime import datetime, timedelta, timezone

from google.cloud import datastore

client = datastore.Client()

q = client.query(kind="ProcessorLog")
t = datetime.utcnow() + timedelta(-1)
t = t.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
print(t, "-->", t + timedelta(1))
q.add_filter("time", ">=", t)
q.add_filter("time", "<=", t + timedelta(1))
for ix, d in enumerate(q.fetch()):
    print(
        ix,
        d["processor_name"],
        d.get("output_fname"),
        d["time"],
        d.get("refdate"),
    )

# print(list(q.fetch()))
