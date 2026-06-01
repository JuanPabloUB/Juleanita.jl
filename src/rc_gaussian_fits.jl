"""
    _fit_rc_gaussian_fits(amplitude_grid, rc_grid, min_amp, max_amp, nbins, rel_cut_fit)

Fit a truncated Gaussian to every RC row in `amplitude_grid`.

This is the Juleanita-local variant of `fit_enc_sigmas` that also keeps the
individual fit reports for diagnostic plots.
"""
function _fit_rc_gaussian_fits(amplitude_grid::Matrix{T}, rc_grid, min_amp::T, max_amp::T, nbins::Int, rel_cut_fit::Real) where T<:Real
    @assert size(amplitude_grid, 1) == length(rc_grid) "amplitude_grid and rc_grid must have the same number of RC points"

    rc_values = collect(rc_grid)
    μ = Vector{Measurement}(undef, length(rc_values))
    σ = Vector{Measurement}(undef, length(rc_values))
    fit_reports = Vector{Any}(undef, length(rc_values))
    fit_success = falses(length(rc_values))

    Threads.@threads for r in eachindex(rc_values)
        rc = rc_values[r]
        amplitudes = filter(isfinite, collect(view(amplitude_grid, r, :)))

        if isempty(amplitudes) || all(amplitudes .== zero(T))
            continue
        end

        try
            cuts = cut_single_peak(amplitudes, min_amp, max_amp; n_bins = nbins, relative_cut = rel_cut_fit)
            result_fit, report_fit = fit_single_trunc_gauss(amplitudes, cuts)

            μ[r] = result_fit.μ
            σ[r] = result_fit.σ
            fit_reports[r] = report_fit
            fit_success[r] = true
        catch e
            @warn "RC Gaussian fit failed for rc = $rc" exception = (e, catch_backtrace())
        end
    end

    if !any(fit_success)
        @error "No valid RC Gaussian fit found"
        throw(ErrorException("No valid RC Gaussian fit found"))
    end

    fit_indices = findall(fit_success)
    rc_success = rc_values[fit_indices]
    μ_success = μ[fit_success]
    σ_success = σ[fit_success]
    report_success = fit_reports[fit_success]

    min_sigma = minimum(σ_success)
    rc_min_sigma = rc_success[findmin(σ_success)[2]]
    rc_step = length(rc_values) > 1 ? abs(rc_values[2] - rc_values[1]) : zero(first(rc_values))

    result = (
        rc_min_sigma = measurement(rc_min_sigma, rc_step),
        min_sigma = min_sigma,
        rc = rc_success,
        μ = μ_success,
        σ = σ_success,
        fit_success = fit_success,
        min_amp = min_amp,
        max_amp = max_amp,
        nbins = nbins,
        rel_cut_fit = rel_cut_fit,
    )
    report = merge(result, (
        fit_indices = fit_indices,
        fit_reports = report_success,
    ))
    return result, report
end
