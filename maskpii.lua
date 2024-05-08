local json = require 'json'

local function maskSSN(ssn)
    if not ssn then
        return nil
    end
    -- Replace all but the last four digits of the SSN with "XXX-XX-"
    return string.gsub(ssn, "^(.-)(%d%d%d%d)$", "XXX-XX-%2")
end

local function RowToMap(row)
    local map = peerdb.RowTable(row)
    for col, val in pairs(map) do
        local kind = peerdb.RowColumnKind(row, col)
        if col == 'ssn' then
            -- Apply the maskSSN function to the SSN column
            map[col] = maskSSN(val)
        elseif kind == 'bytes' or kind == 'bit' then
            map[col] = json.bin(val)
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
    return json.encode(record)
end
