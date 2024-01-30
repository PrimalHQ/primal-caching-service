##
@time pks = collect(keys(cache_storage.meta_data))
##
running = Utils.PressEnterToStop()
errs = Ref(0)
ttotal = Ref(0.0)
tstart = time()
@time for (i, pk) in enumerate(pks)
    yield()
    running[] || break
    i % 1000 == 0 && print("$i  $(time()-tstart)s  $((time()-tstart)/i)s/e    \r")
    try
        ttotal[] += @elapsed DB.update_pubkey_ln_address(cache_storage, pk)
    catch ex
        errs[] += 1
        #Utils.print_exceptions()
        println(ex)
    end
end
errs[], ttotal[], ttotal[]/length(pks)
##
