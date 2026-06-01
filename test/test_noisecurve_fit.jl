using Juleanita
using Test
using Measurements
using Measurements: value as mvalue, uncertainty as muncert
using Random

# Reference ENC values were produced by the awk core of the LUIGI noise bash
# script for the parameter set below (SI units):
#   vn = 10 nV/√Hz, C_in = 4 pF, Rf = 1 GΩ, I_L = 1 pA, f1f = 100 Hz, T = 300 K
# Command (per τ):
#   echo "<τ> 4e-12 10e-9 1e9 1e-12 100 300 0 0.105 2.95" | awk '<script core>'
const _REF_PARAMS = (vn = 10e-9, IL = 1e-12, f1f = 100.0, C_in = 4e-12, Rf = 1e9, T = 300.0)

@testset "CSA noise model" begin

    @testset "enc_model matches bash reference" begin
        @test enc_model(1e-6; _REF_PARAMS...) ≈ 240.8327226 rtol = 1e-6
        @test enc_model(5e-6; _REF_PARAMS...) ≈ 120.5550013 rtol = 1e-6
    end

    @testset "enc_components matches bash reference (τ = 1 µs)" begin
        c = enc_components(1e-6; _REF_PARAMS...)
        @test c.series   ≈ 239.4982144 rtol = 1e-6
        @test c.feedback ≈ 24.36533881 rtol = 1e-6
        @test c.leakage  ≈ 3.389132454 rtol = 1e-6
        @test c.over_f   ≈ 5.98745536  rtol = 1e-6
        @test c.total    ≈ 240.8327226 rtol = 1e-6
    end

    @testset "enc_components matches bash reference (τ = 5 µs)" begin
        c = enc_components(5e-6; _REF_PARAMS...)
        @test c.series   ≈ 107.1068576 rtol = 1e-6
        @test c.feedback ≈ 54.48255387 rtol = 1e-6
        @test c.leakage  ≈ 7.578330551 rtol = 1e-6
        @test c.over_f   ≈ 5.98745536  rtol = 1e-6
        @test c.total    ≈ 120.5550013 rtol = 1e-6
    end

    @testset "1/f term is τ-independent" begin
        c1 = enc_components(1e-6; _REF_PARAMS...)
        c5 = enc_components(5e-6; _REF_PARAMS...)
        @test c1.over_f ≈ c5.over_f rtol = 1e-12
    end

    @testset "fwhm_kev_model scales ENC by 2.355·We/1000" begin
        We = 2.95
        @test fwhm_kev_model(1e-6; _REF_PARAMS..., We = We) ≈
            240.8327226 * 2.355 * We / 1000 rtol = 1e-6
    end

    @testset "vector broadcast" begin
        τ = [1e-6, 5e-6]
        @test enc_model(τ; _REF_PARAMS...) ≈ [240.8327226, 120.5550013] rtol = 1e-6
    end

    @testset "round-trip parameter recovery" begin
        We  = 2.95
        τ_s = collect((0.2:0.2:9.0) .* 1e-6)   # mirrors the DSP e_grid_rt_trap sweep
        truth = (vn = 12e-9, IL = 50e-12, f1f = 80.0, C_in = 4e-12, Rf = 1e9, T = 295.0)

        fwhm_true = fwhm_kev_model(τ_s; truth..., We = We)
        Random.seed!(42)
        rel_err   = 0.01
        fwhm_noisy = fwhm_true .* (1 .+ rel_err .* randn(length(τ_s)))
        fwhm_meas  = measurement.(fwhm_noisy, rel_err .* fwhm_true)

        res = fit_csa_noise_model(τ_s, fwhm_meas;
                                  C_in = truth.C_in, Rf = truth.Rf, T = truth.T, We = We)

        @test mvalue(res.vn)  ≈ truth.vn  rtol = 0.1
        @test mvalue(res.f1f) ≈ truth.f1f rtol = 0.3
        @test mvalue(res.IL)  ≈ truth.IL  atol = 3 * muncert(res.IL)
        @test res.χ²_dof < 5
    end
end
