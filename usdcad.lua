local json = require "json"

local function RowToMap(row)
	if not row then
		return
	end
	local map = peerdb.RowTable(row)
	map.salary_in_cad = map.salary_in_usd * 1.4
	return map
end

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
		before = RowToMap(record.old),
		after = RowToMap(record.new),
		commitms = record.commit_time.unix_milli,
		table = record.source,
		lsn = record.checkpoint,
	}
end