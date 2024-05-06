local bit32 = require "bit32"
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
	local topic
	if bit32.btest(record.row.id, 1) then
		topic = "odd"
	else
		topic = "even"
	end
	return {
		topic = topic,
		value = json.encode {
			op = op,
			before = record.old,
			after = record.new,
			commitms = record.commit_time.unix_milli,
			table = record.source,
			lsn = record.checkpoint,
		}
	}
end