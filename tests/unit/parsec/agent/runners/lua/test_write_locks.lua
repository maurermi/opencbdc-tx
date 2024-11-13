function gen_bytecode()
    pay_contract = function(param)
        Locks = {
            READ = 0,
            WRITE = 1,
        }
        data2 = coroutine.yield("W", Locks.WRITE)
        return "Done"
    end
    c = string.dump(pay_contract, true)
    t = {}
    for i = 1, #c do
        t[#t + 1] = string.format("%02x", string.byte(c, i))
    end

    return table.concat(t)
end
