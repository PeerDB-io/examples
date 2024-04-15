local msgpack = require 'msgpack'

local function RowToMap(row)
	local map = peerdb.RowTable(row)
	for col, val in pairs(map) do
		local kind = peerdb.RowColumnKind(row, col)
		if kind == 'numeric' then
			map[col] = msgpack.ext(10, msgpack.encode(val.exponent) .. val.coefficient.bytes)
		elseif kind == 'bytes' or kind == 'bit' then
			map[col] = msgpack.bin(val)
		end
	end
	return map
end

local RKINDMAP = {
	insert = string.byte('i', 1),
	update = string.byte('u', 1),
	delete = string.byte('d', 1),
}

function onRecord(r)
	local kind = RKINDMAP[r.kind]
	if not kind then
		return
	end
	local record = {
		action = kind,
		lsn = r.checkpoint,
		time = r.commit_time,
		source = r.source,
	}
	if r.old then
		record.old = RowToMap(r.old)
	end
	if r.new then
		record.new = RowToMap(r.new)
	end
	return msgpack.encode(record)
end