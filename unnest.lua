local json = require "json"

local function RowToMap(row)
	if not row then
		return
	end
	local map = peerdb.RowTable(row)
	local map2 = {} -- pairs breaks if new keys added to map
	for col, val in pairs(map) do
		local kind = peerdb.RowColumnKind(row, col)
		if kind == 'json' then
			local jsonval = json.decode(val)
			local submap = json.unmark(jsonval)
			if type(submap) == "table" then
				for subcol, subval in pairs(submap) do
					map2[subcol] = subval
				end
				map[col] = nil
			else
				map[col] = submap
			end
		end
	end
	for col, val in pairs(map2) do
		map[col] = val
	end
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