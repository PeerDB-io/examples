local msgpack = require 'msgpack'

local function RowToMap(row)
	local cols = peerdb.RowColumns(row)
	local map = {}
	for _, col in ipairs(cols) do
		local kind = peerdb.RowColumnKind(row, col)
		if string.sub(kind, 1, #'array_') == 'array_' then
			map[col] = msgpack.array(row[col])
		elseif kind == 'numeric' then
			local dec = row[col]
			map[col] = msgpack.ext(10, msgpack.encode(dec.exponent) .. dec.coefficient.bytes)
		elseif kind == 'bytes' or kind == 'bit' then
			map[col] = msgpack.bin(row[col])
		else
			map[col] = row[col]
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
		action = r.kind,
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