request= function()
    local uuid = io.open("/proc/sys/kernel/random/uuid", "r"):read()
    local data = [[{
        "key": "%s",
        "value": "%s"
        }]]
    local f = io.open('set_data', "a")
    io.output(f)
    io.write(string.format("%s\n", tostring(uuid)))
    io.close(f)
    wrk.method = "POST"
    wrk.body   = string.format(data,tostring(uuid),tostring(uuid))
    wrk.headers["Content-Type"] = "application/json"
    return wrk.format()
end