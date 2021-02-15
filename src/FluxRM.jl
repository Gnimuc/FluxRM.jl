module FluxRM

include("api.jl")

export Flux

function version()
    major = Ref{Cint}()
    minor = Ref{Cint}()
    patch = Ref{Cint}()

    API.flux_core_version(major, minor, patch)
    Base.VersionNumber(major[], minor[], patch[])
end

mutable struct Flux
    handle::Ptr{API.flux_t}

    function Flux(handle::Ptr{API.flux_t})
        @assert handle != C_NULL
        this = new(handle)
        finalizer(this) do flux
            API.flux_close(flux)
        end
        return this
    end
end
Base.unsafe_convert(::Type{Ptr{API.flux_t}}, flux::Flux) = flux.handle
# flux_fatal_set

function Flux(uri = nothing; flags = 0)
    if uri === nothing
        uri = C_NULL
    end
    handle = API.flux_open(uri, flags)
    Libc.systemerror("flux_open", handle == C_NULL)
    Flux(handle)
end

function Base.copy(flux::Flux)
    handle = API.flux_clone(flux)
    Flux(handle)
end

function rank(flux::Flux)
    r_rank = Ref{UInt32}()
    API.flux_get_rank(flux, r_rank)
    r_rank[]
end

function size(flux::Flux)
    r_size = Ref{UInt32}()
    API.flux_get_rank(flux, r_size)
    r_size[]
end

function Base.getindex(flux::Flux, key::String)
    str = API.flux_attr_get(flux, key)
    Base.unsafe_string(str)
end

function Base.setindex!(flux::Flux, value::String, key::String)
    err = API.flux_attr_set(flux, key, value)
    Libc.systemerror("flux_attr_set", err == -1)
    value
end

mutable struct Future
    handle::API.flux_future_t
    refs::IdDict{Any, Nothing}
    function Future(handle)
        this = new(handle, IdDict{Any, Nothing}())
        finalizer(this) do future
            API.flux_future_destroy(future.handle)
        end
        return this
    end
end

function lookup(flux::Flux, key, ns=C_NULL, flags=0)
    handle = API.flux_kvs_lookup(flux, ns, flags, key)
    Libc.systemerror("flux_kvs_lookup", handle == C_NULL)
    return Future(handle)
end

function get(future::Future)
    data = Ref{Ptr{Void}}()
    len = Ref{Cint}()
    API.flux_kvs_lookup_get_raw(future.handle, data, len)
    Base.unsafe_string(data[], len[])
end

mutable struct Transaction
    handle::API.flux_kvs_txn_t
    function Transaction()
        handle = API.flux_kvs_txn_create()
        this = new(handle)
        finalizer(this) do txn
            API.flux_kvs_txn_destroy(txn.handle)
        end
    end
    return this
end

function commit(flux::Flux, txn::Transaction, ns=C_NULL; flags=0)
    handle = API.flux_kvs_commit(flux, ns, flags, txn.handle)
    Libc.systemerror("flux_kvs_commit", handle == C_NULL)
    fut = Future(handle)
    fut.refs[txn] = nothing # root txn in fut
    return fut
end

function fence(flux::Flux, txn::Transaction, name, nprocs, ns=C_NULL; flags=0)
    handle = API.flux_kvs_fence(flux, ns, flags, name, nprocs, txn.handle)
    Libc.systemerror("flux_kvs_fence", handle == C_NULL)
    fut = Future(handle)
    fut.refs[txn] = nothing # root txn in fut
    return fut
end

mutable struct KVS
    flux::Flux
    namespace::String
    current_txn::Transaction
    lock::Base.ReentrantLock()
    function KVS(flux::Flux, namespace)
        new(flux, namespace, Transaction(), Base.ReentrantLock())
    end
end
function exchange_transaction!(kvs::KVS)
    txn = lock(kvs) do
        txn = kvs.current_txn
        kvs.current_txn = Transaction()
        txn
    end
    return txn
end

function commit(kvs::KVS)
    txn = exchange_transaction!(kvs)
    commit(kvs.flux, txn, kvs.namespace)
end

function fence(kvs::KVS, name, nprocs)
    txn = exchange_transaction!(kvs)
    fence(kvs.flux, txn, name, nprocs, kvs.namespace)
end

function lookup(kvs::KVS, key)
    lookup(kvs.flux, key, kvs.namespace)
end

end # module
