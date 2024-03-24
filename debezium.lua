-- This schema agnostic script outputs json according to Debezium's serde format:
-- https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-events
-- note PeerDB only supports a subset of this; truncate for example is ignored

local json = require "json"

local OPMAP = {
	insert = "c",
	update = "u",
	delete = "d",
}

function onRecord(record)
	local op = OPMAP[record.kind]
	if not op then
		return
	end
	return json.encode {
		op = op,
		ts_ms = peerdb.Now().unix_milli,
		before = json.encode(record.old),
		after = json.encode(record.new),
		source = {
			version = "PeerDB",
			to_ms = record.commit_time.unix_milli,
			table = record.source,
			lsn = record.checkpoint,
		}
	}
end