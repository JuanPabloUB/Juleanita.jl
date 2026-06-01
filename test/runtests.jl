using Juleanita
using Test

@testset "Juleanita.jl" begin
    include("test_io_caen.jl")
    include("test_noisecurve_fit.jl")
end
