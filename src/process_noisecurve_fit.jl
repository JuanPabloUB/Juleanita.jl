# Goulding/Radeka CSA noise-model fit on the intrinsic FWHM curve produced by
# process_noisecurve.jl.
#
# Reduction trick: multiplying the squared model by τ turns the noise
# (constant/τ + linear in τ + constant) into a degree-2 polynomial in τ,
# linear in its three coefficients. The fit is solved by weighted normal
# equations in closed form (global optimum + full covariance).

# Physical constants (SI). `_NOISE_E_CONST` is the Gaussian-shaper noise
# prefactor (e²/8 ≈ 0.921).
const _NOISE_E_CONST = 2.713
const _NOISE_K_B     = 1.38e-23
const _NOISE_Q       = 1.602e-19

# LEGEND house palette (LegendMakie utils.jl), hard-coded so the plot matches
# the collaboration style without depending on color-constant exports.
const _COL_DATA     = "#07A9FF"   # AchatBlue / ICPCBlue
const _COL_SERIES   = "#BF00BF"   # PPCPurple
const _COL_FEEDBACK = "#FFA500"   # BEGeOrange
const _COL_LEAKAGE  = "#008000"   # CoaxGreen
const _COL_1F       = "#1A2A5B"   # DeepCove

"""
    enc_model(τ; vn, IL, f1f, C_in, Rf, T)

Goulding/Radeka CSA noise ENC (electrons RMS) at shaping time `τ` (seconds).

Inputs (SI):
- `vn`   — input voltage-noise density [V/√Hz]
- `IL`   — input leakage current [A]
- `f1f`  — 1/f corner frequency [Hz]
- `C_in` — total input capacitance [F]
- `Rf`   — feedback resistance [Ω]
- `T`    — operating temperature [K]

Accepts scalar or vector `τ`. Result broadcasts to the shape of `τ`.
"""
function enc_model(τ; vn, IL, f1f, C_in, Rf, T)
    e2_8 = _NOISE_E_CONST^2 / 8
    enc_sq = @. e2_8 * (
          vn^2 * C_in^2 / τ
        + τ * 4 * _NOISE_K_B * T / Rf
        + τ * 2 * _NOISE_Q * IL
        + 6.25 * f1f * vn^2 * C_in^2
    ) / _NOISE_Q^2
    return @. sqrt(enc_sq)
end
export enc_model

"""
    enc_components(τ; vn, IL, f1f, C_in, Rf, T)

Per-component ENC decomposition (electrons RMS) for plot overlays:
`(series, feedback, leakage, over_f, total)`. Same kwargs as `enc_model`.
"""
function enc_components(τ; vn, IL, f1f, C_in, Rf, T)
    e2_8 = _NOISE_E_CONST^2 / 8
    q2   = _NOISE_Q^2
    series   = @. sqrt(e2_8 * vn^2 * C_in^2 / τ / q2)
    feedback = @. sqrt(e2_8 * τ * 4 * _NOISE_K_B * T / Rf / q2)
    leakage  = @. sqrt(e2_8 * τ * 2 * _NOISE_Q * IL / q2)
    # `+ zero(τ)` broadcasts the 1/f scalar to the shape of τ.
    over_f   = @. sqrt(e2_8 * 6.25 * f1f * vn^2 * C_in^2 / q2) + zero(τ)
    total    = @. sqrt(series^2 + feedback^2 + leakage^2 + over_f^2)
    return (series = series, feedback = feedback, leakage = leakage,
            over_f = over_f, total = total)
end
export enc_components

"""
    fwhm_kev_model(τ; vn, IL, f1f, C_in, Rf, T, We)

CSA FWHM at shaping time `τ` (seconds), in keV. `We` is the mean energy per
electron–hole pair in **eV** (e.g. 2.95 for Ge at 295 K, 3.7 for Si).
"""
fwhm_kev_model(τ; vn, IL, f1f, C_in, Rf, T, We) =
    enc_model(τ; vn = vn, IL = IL, f1f = f1f, C_in = C_in, Rf = Rf, T = T) .*
        (2.355 * We / 1000)
export fwhm_kev_model

"""
    fit_csa_noise_model(τ_s, fwhm_kev; C_in, Rf, T, We)

Fit the Goulding/Radeka CSA noise model to (`τ_s`, `fwhm_kev`) and return
physical `vn`, `IL`, `f1f` as `Measurement` values together with diagnostics.

Inputs:
- `τ_s`      — shaping times in **seconds** (not µs).
- `fwhm_kev` — intrinsic CSA FWHM in keV per τ, as `Measurement`.
- `C_in` [F], `Rf` [Ω], `T` [K] — fixed metadata.
- `We` [eV] — energy per electron–hole pair.
- `Rf_rel_err` — fractional 1σ uncertainty on `Rf` (e.g. 0.05 for 5 %). Folded
  into `I_L` via Monte Carlo; 0 means `Rf` is treated as exact.
- `n_mc`, `rng_seed` — Monte-Carlo sample count and seed (reproducible).

Method: `vn` and `f1f` come from a degree-2 polynomial fit of `y = τ·FWHM²`
in `τ_us`-space. The model is exactly linear in `(a,b,c)`, so we solve the
weighted normal equations directly — guaranteed global optimum with full
covariance, no iterative optimizer. `vn` and `f1f` follow from an analytic
back-transform of `a, b` (Measurement propagation).

`I_L` comes from the parallel coefficient `c`: since `Rf` is 100 % shape-
degenerate with `I_L`, the feedback Johnson term `4kT/Rf` is subtracted from
`c` by Monte Carlo over `c` (Gaussian from the fit) and `Rf` (Gaussian prior,
width `Rf_rel_err`), with the physical `I_L ≥ 0` boundary enforced. Because
`Rf` enters nowhere but `c`, this is equivalent to a full joint Bayesian fit
for `I_L`. `I_L` is reported as a 95 % upper limit when consistent with zero.
"""
function fit_csa_noise_model(τ_s::AbstractVector{<:Real},
                              fwhm_kev::AbstractVector{<:Measurement};
                              C_in::Real, Rf::Real, T::Real, We::Real,
                              Rf_rel_err::Real = 0.0, n_mc::Int = 100_000, rng_seed::Int = 1234)
    length(τ_s) == length(fwhm_kev) ||
        throw(ArgumentError("τ_s and fwhm_kev length mismatch ($(length(τ_s)) vs $(length(fwhm_kev)))"))
    length(τ_s) >= 4 ||
        throw(ArgumentError("need at least 4 τ points to fit 3 parameters; got $(length(τ_s))"))

    # K folds the FWHM-per-electron factor, the shaper prefactor e²/8 and the
    # charge→electron conversion 1/q² into one constant, so that
    #   FWHM²(τ_s) = K · [vn²·C_in²/τ_s + (4kT/Rf + 2q·I_L)·τ_s + 6.25·f1f·vn²·C_in²].
    K = (2.355 * We / 1000)^2 * _NOISE_E_CONST^2 / 8 / _NOISE_Q^2

    # Fit in µs, not seconds: in SI the polynomial coefficients span ~10 orders
    # of magnitude (a~1e-6, c~1e4), poorly conditioned. In µs they are all
    # O(1e-3…1e0). τ_scale converts back: τ_s = τ_us · τ_scale.
    τ_scale = 1e-6
    τ_us    = τ_s ./ τ_scale

    # Transform: y(τ_us) = τ_us · FWHM²(τ). With τ_s = τ_us·τ_scale the model is a
    # degree-2 polynomial in τ_us:
    #   y = K·P/τ_scale  +  K·S · τ_us  +  K·Q·τ_scale · τ_us²
    # where P = vn²·C_in², S = 6.25·f1f·vn²·C_in², Q = 4kT/Rf + 2q·I_L.
    y_meas = τ_us .* (fwhm_kev .^ 2)

    # Solve the weighted linear least squares directly. The model is exactly
    # linear in the basis [1, τ_us, τ_us²], so the normal equations give the
    # global optimum in closed form with its covariance — no iterative
    # optimizer required.
    X   = hcat(ones(length(τ_us)), τ_us, τ_us .^ 2)
    yv  = mvalue.(y_meas)
    σy  = muncert.(y_meas)
    w   = [ (isfinite(s) && s > 0) ? inv(s^2) : 1.0 for s in σy ]   # equal-weight fallback
    XtW    = transpose(X) * Diagonal(w)
    covpar = inv(Symmetric(XtW * X))
    pvec   = covpar * (XtW * yv)
    perr   = sqrt.(diag(covpar))
    a = measurement(pvec[1], perr[1])
    b = measurement(pvec[2], perr[2])
    c = measurement(pvec[3], perr[3])
    chi2_dof = sum(((yv .- X * pvec) ./ σy) .^ 2) / (length(τ_us) - 3)

    # vn and f1f: analytic back-transform (Measurement errors propagate exactly).
    # They come from a and b only — independent of Rf.
    P = a * τ_scale / K        # vn²·C_in²
    S = b / K                  # 6.25·f1f·vn²·C_in²
    mvalue(P) > 0 || @warn "fitted series coefficient ≤ 0 (a = $(a)); vn will be NaN — check data/metadata"
    vn  = sqrt(P) / C_in       # V/√Hz
    f1f = S / (6.25 * P)       # Hz (independent of C_in)

    # I_L: subtract the feedback Johnson term from the parallel total Q = c/(K·τ_scale)
    # (the SI-unit parallel-noise coefficient), propagating the uncertain Rf by
    # Monte Carlo (I_L ≥ 0 enforced).
    Q = c / (K * τ_scale)        # 4kT/Rf + 2q·I_L  in SI units
    IL, IL_is_upper_limit, IL_upper_95 =
        _leakage_posterior(Q, T, Rf, Rf_rel_err; n_mc = n_mc, rng_seed = rng_seed)

    return (
        vn  = vn,
        IL  = IL,
        f1f = f1f,
        IL_is_upper_limit = IL_is_upper_limit,
        IL_upper_95 = IL_upper_95,
        χ²_dof = chi2_dof,
        K = K,
        poly_par = [a, b, c],
        poly_cov = covpar,
        C_in = C_in, Rf = Rf, T = T, We = We,
        τ_s = collect(τ_s), fwhm_kev = collect(fwhm_kev),
    )
end
export fit_csa_noise_model

"""
    _leakage_posterior(Q, T, Rf, Rf_rel_err; n_mc, rng_seed)

Monte-Carlo posterior of the leakage current `I_L`. `Q` is the SI-unit
parallel-noise total from the fit, i.e. `Q = 4kT/Rf + 2q·I_L` (a Measurement);
the caller must have already converted from the polynomial coefficient.

`I_L = (Q − 4kT/Rf)/(2q)`. We sample `Q` from its fit Gaussian and `Rf` from a
Gaussian prior of fractional width `Rf_rel_err`, compute `I_L` per draw, and
impose the physical `I_L ≥ 0` boundary by truncation.

Returns `(IL::Measurement, is_upper_limit::Bool, upper_95::Float64)`:
- `IL` — median of the truncated posterior ± half its central 68 % interval.
- `is_upper_limit` — true when > 5 % of draws fall below zero (consistent with
  no detectable leakage).
- `upper_95` — 95th percentile of the truncated posterior [A].
"""
function _leakage_posterior(Q, T, Rf, Rf_rel_err; n_mc::Int = 100_000, rng_seed::Int = 1234)
    rng  = Random.Xoshiro(rng_seed)
    Q_s  = mvalue(Q) .+ muncert(Q) .* randn(rng, n_mc)
    Rf_s = Rf_rel_err > 0 ? Rf .* (1 .+ Rf_rel_err .* randn(rng, n_mc)) : fill(float(Rf), n_mc)
    IL_s = (Q_s .- 4 .* _NOISE_K_B .* T ./ Rf_s) ./ (2 * _NOISE_Q)   # may be negative

    frac_neg = count(<(0), IL_s) / n_mc
    pos = filter(≥(0), IL_s)                 # posterior under the I_L ≥ 0 prior
    isempty(pos) && (pos = [0.0])
    med = StatsBase.quantile(pos, 0.5)
    lo  = StatsBase.quantile(pos, 0.16)
    hi  = StatsBase.quantile(pos, 0.84)
    return measurement(med, (hi - lo) / 2), frac_neg > 0.05, StatsBase.quantile(pos, 0.95)
end

"""
    _plot_noisecurve_fit(fitres; title, subtitle)

Overlay the fitted CSA noise model and its component decomposition on the
intrinsic FWHM data points. `fitres` is the NamedTuple returned by
`fit_csa_noise_model`. Mirrors the LEGEND-style two-panel layout (top: model
+ data + components; bottom: σ-normalised residuals with shaded ±1σ/±3σ bands).
"""
function _plot_noisecurve_fit(fitres; title = "", subtitle = "")
    τ_us     = fitres.τ_s .* 1e6
    fwhm     = mvalue.(fitres.fwhm_kev)
    fwhm_err = muncert.(fitres.fwhm_kev)

    # Central-value parameters; I_L clamped ≥ 0 for the overlay/leakage curve.
    pc = (vn = mvalue(fitres.vn), IL = max(mvalue(fitres.IL), 0.0), f1f = mvalue(fitres.f1f),
          C_in = fitres.C_in, Rf = fitres.Rf, T = fitres.T)
    scale = 2.355 * fitres.We / 1000   # electrons → keV FWHM

    # Normalised residuals (data − model) / σ_data, evaluated at the data points.
    fwhm_fit_pts   = fwhm_kev_model(fitres.τ_s; pc..., We = fitres.We)
    residuals_norm = (fwhm .- fwhm_fit_pts) ./ fwhm_err

    # Smooth model curves on a fine grid.
    τ_fine    = collect(range(minimum(fitres.τ_s), maximum(fitres.τ_s), length = 300))
    τ_fine_us = τ_fine .* 1e6
    comps     = enc_components(τ_fine; pc...)

    xlims = (minimum(τ_us) - 0.2, maximum(τ_us) + 0.2)

    fig = Figure(size = (900, 600))
    g   = Makie.GridLayout(fig[1, 1])
    ax  = Axis(g[1, 1], title = title, subtitle = subtitle,
               ylabel = "ENC FWHM (keV)", limits = (xlims, (0, nothing)))

    # Draw the model curves first so they sit BEHIND the data points.
    l_total    = lines!(ax, τ_fine_us, comps.total    .* scale, color = :red,          linewidth = 3)
    l_series   = lines!(ax, τ_fine_us, comps.series   .* scale, color = _COL_SERIES,   linewidth = 2, linestyle = :dash)
    l_feedback = lines!(ax, τ_fine_us, comps.feedback .* scale, color = _COL_FEEDBACK, linewidth = 2, linestyle = :dash)
    l_leakage  = lines!(ax, τ_fine_us, comps.leakage  .* scale, color = _COL_LEAKAGE,  linewidth = 2, linestyle = :dash)
    l_1f       = lines!(ax, τ_fine_us, comps.over_f   .* scale, color = _COL_1F,       linewidth = 2, linestyle = :dot)
    # Data on top, in LEGEND blue.
    Makie.errorbars!(ax, τ_us, fwhm, fwhm_err, color = (_COL_DATA, 0.7))
    s_data = Makie.scatter!(ax, τ_us, fwhm, color = _COL_DATA)
    # Legend order is set explicitly so CSA FWHM stays at the top despite the draw order.
    Legend(g[1, 2],
           [s_data, l_total, l_series, l_feedback, l_leakage, l_1f],
           ["CSA FWHM intrinsic", "fit total", "series (vₙ·C_in)", "feedback (R_f)", "leakage (I_L)", "1/f"])

    # Residual panel (σ), LegendMakie style: ±3σ light-grey, ±1σ dark-grey bands.
    ax2 = Axis(g[2, 1], yticks = -3:3:3, limits = (xlims, (-5, 5)),
               xlabel = "RC time constant (µs)", ylabel = "Residuals (σ)")
    Makie.hspan!(ax2, -3, 3, color = :lightgrey)
    Makie.hspan!(ax2, -1, 1, color = :darkgrey)
    Makie.scatter!(ax2, τ_us, residuals_norm, color = :black, markersize = 8)

    # Hide the top axis' x decorations, link x, give the main panel 4× the height.
    ax.xticklabelsize = 0
    ax.xticksize = 0
    Makie.linkxaxes!(ax, ax2)
    Makie.rowgap!(g, 0)
    Makie.rowsize!(g, 1, Makie.Auto(4))
    return fig
end

"""
    _noise_fit_subtitle(fitres)

Compact summary line of the fitted parameters for the plot subtitle.
"""
function _noise_fit_subtitle(fitres)
    vn_nv  = mvalue(fitres.vn) * 1e9
    vn_err = muncert(fitres.vn) * 1e9
    f1f_v  = mvalue(fitres.f1f)
    f1f_e  = muncert(fitres.f1f)
    il_str = if fitres.IL_is_upper_limit
        @sprintf("I_L < %.2f pA (95%% CL)", fitres.IL_upper_95 * 1e12)
    else
        @sprintf("I_L = %.2f ± %.2f pA", mvalue(fitres.IL) * 1e12, muncert(fitres.IL) * 1e12)
    end
    @sprintf("vₙ = %.2f ± %.2f nV/√Hz,  f₁/f = %.0f ± %.0f Hz,  %s,  χ²/dof = %.2f",
             vn_nv, vn_err, f1f_v, f1f_e, il_str, fitres.χ²_dof)
end

"""
    process_noisecurve_fit(data, period, run, category, channel, dsp_config; kwargs...)

Fit the Goulding/Radeka CSA noise model to the intrinsic FWHM curve of a run
and persist the physical parameters (`vn`, `I_L`, `f1f`) to `rpars.noise_fit`.
The intrinsic curve is reconstructed from the pickoff-mode `rpars.noise`
results produced by [`process_noisecurve`](@ref) (invoked here in cached mode
to assemble the report and pair the pulser floor).

Keyword arguments:
- `waveform_type` (= `:waveform`) — passed to `process_noisecurve`.
- `Rf` — feedback resistance [Ω]. Falls back to hardware metadata
  `r_feedback`; one of the two must be available.
- `Rf_err_percent` — percent 1σ uncertainty on `Rf`, folded into the `I_L`
  Monte Carlo. Falls back to metadata `r_feedback_err_percent`, else 0.
- `cap_input_residual` — lumped FET-gate + parasitic input capacitance [F].
  Falls back to hardware metadata `cap_input_residual`, else 0 (warned).
- `We` — energy per e–h pair [eV]. Defaults to `Ge_Energy_per_eholePair(T)`,
  the same value the noise curve plot uses (the choice cancels out of the
  extracted noise parameters as long as it matches the data conversion).
- `reprocess` — recompute and overwrite an existing `rpars.noise_fit` entry.
"""
function process_noisecurve_fit(data::LegendData, period::DataPeriod, run::DataRun,
                                 category::Union{Symbol, DataCategory}, channel::ChannelId,
                                 dsp_config::DSPConfig;
                                 waveform_type::Symbol = :waveform,
                                 Rf::Union{Nothing, Real} = nothing,
                                 Rf_err_percent::Union{Nothing, Real} = nothing,
                                 cap_input_residual::Union{Nothing, Real} = nothing,
                                 We::Union{Nothing, Real} = nothing,
                                 reprocess::Bool = false)
    fit_key = Symbol("rc_$(waveform_type)_pickoff_modelfit")

    # Short-circuit if already done.
    if !reprocess && isfile(joinpath(data_path(data.par[category].rpars.noise_fit[period]), "$run.json"))
        existing = data.par[category].rpars.noise_fit[period, run, channel]
        if haskey(existing, fit_key)
            @info "Load cached CSA noise-model fit for $category-$period-$run-$channel"
            return existing[fit_key]
        end
    end

    # Assemble the intrinsic-FWHM report via the existing pickoff pipeline
    # (cached: this reads rpars.noise + pairs the pulser floor from ecal).
    _, report_rc = process_noisecurve(data, period, run, category, channel, dsp_config;
                                      waveform_type = waveform_type, rt_opt_mode = :pickoff,
                                      reprocess = false)

    # Hardware metadata.
    filekeys = search_disk(FileKey, data.tier[DataTier(:raw), category, period, run])
    filekey  = first(filekeys)
    hw_meta  = getproperty(data.metadata.hardware, Symbol(data.name))(filekey)

    V_ref          = uconvert(u"V", hw_meta.V_ref)
    gain           = hw_meta.gain_tot
    cap_feedback_F = ustrip(u"F", hw_meta.cap_feedback)
    cap_inj_F      = ustrip(u"F", hw_meta.cap_inj)
    temp_K         = ustrip(u"K", hw_meta.operating_temperature)
    W_eh           = isnothing(We) ? Ge_Energy_per_eholePair(temp_K) : float(We)

    # C_in = feedback + injection + residual (+ detector, when attached).
    cap_residual_F = if !isnothing(cap_input_residual)
        float(cap_input_residual)
    elseif hasproperty(hw_meta, :cap_input_residual)
        ustrip(u"F", hw_meta.cap_input_residual)
    else
        @warn "no cap_input_residual in metadata or kwargs — C_in uses feedback+injection only; " *
              "vₙ will be a lower bound. Add cap_input_residual to the hardware config."
        0.0
    end
    cap_detector_F = hasproperty(hw_meta, :cap_detector) ? ustrip(u"F", hw_meta.cap_detector) : 0.0
    C_in = cap_feedback_F + cap_inj_F + cap_residual_F + cap_detector_F

    # Rf (central value) and its fractional uncertainty. The error feeds the
    # Monte-Carlo I_L extraction; metadata stores it as `r_feedback_err_percent`.
    Rf_Ω = if !isnothing(Rf)
        float(Rf)
    elseif hasproperty(hw_meta, :r_feedback)
        ustrip(u"Ω", hw_meta.r_feedback)
    else
        error("feedback resistance unavailable: pass `Rf` or add `r_feedback` to the hardware config")
    end
    Rf_rel_err = if !isnothing(Rf_err_percent)
        Rf_err_percent / 100
    elseif hasproperty(hw_meta, :r_feedback_err_percent)
        hw_meta.r_feedback_err_percent / 100
    else
        0.0
    end

    # Reconstruct the intrinsic FWHM (keV, Measurement) exactly as
    # plot_noisecurve_pickoff does, but keeping uncertainties for the fit.
    µ_pickoff     = mvalue.(report_rc.µ_pickoff)
    adc_to_V_DAQ  = ustrip(u"V", V_ref) ./ µ_pickoff
    y_scale       = _noisecurve_yscale(adc_to_V_DAQ, :keV; gain = gain,
                                       cap_feedback = cap_feedback_F, W_eh = W_eh)
    y_total       = 2.355 .* report_rc.enc .* y_scale
    y_pulser      = measurement.(report_rc.pulser_fwhm_adc, report_rc.pulser_fwhm_adc_err) .* y_scale
    fwhm2_intr    = y_total .^ 2 .- y_pulser .^ 2          # signed (quadrature subtraction)

    # Keep only finite, positive-variance points (artefactual negatives, where
    # common-mode pickup over-subtracts, are dropped from the fit).
    keep = findall(p -> isfinite(mvalue(p)) && mvalue(p) > 0, fwhm2_intr)
    length(keep) >= 4 ||
        error("only $(length(keep)) usable intrinsic points after quadrature subtraction — cannot fit")
    fwhm_intrinsic = sqrt.(fwhm2_intr[keep])
    τ_s            = ustrip.(u"s", report_rc.rc[keep])

    fitres = fit_csa_noise_model(τ_s, fwhm_intrinsic;
                                 C_in = C_in, Rf = Rf_Ω, T = temp_K, We = W_eh,
                                 Rf_rel_err = Rf_rel_err)

    if fitres.χ²_dof > 5
        @warn "CSA noise-model fit has χ²/dof = $(round(fitres.χ²_dof, digits=2)) > 5 — " *
              "check cap_input_residual / r_feedback in the hardware metadata"
    end

    # Persist physical parameters.
    result = PropDict(
        :vn                => fitres.vn,
        :IL                => fitres.IL,
        :f1f               => fitres.f1f,
        :IL_is_upper_limit => fitres.IL_is_upper_limit,
        :IL_upper_95       => fitres.IL_upper_95,
        :chi2_dof          => fitres.χ²_dof,
        :C_in              => C_in,
        :Rf                => Rf_Ω,
        :Rf_rel_err        => Rf_rel_err,
        :T                 => temp_K,
        :We                => W_eh,
        :n_points          => length(keep),
    )
    existing_pd = if isfile(joinpath(data_path(data.par[category].rpars.noise_fit[period]), "$run.json"))
        data.par[category].rpars.noise_fit[period, run, channel]
    else
        PropDict()
    end
    existing_pd = merge(existing_pd, Dict(fit_key => result))
    writelprops(data.par[category].rpars.noise_fit[period], run, PropDict("$channel" => existing_pd))
    @info "Save CSA noise-model fit to rpars.noise_fit ($category-$period-$run-$channel)"

    # Plot.
    plt_header = get_plottitle(filekey, _channel2detector(data, channel), "RC noise-model fit")
    plt_folder = LegendDataManagement.LDMUtils.get_pltfolder(data, filekey, :noisecurve_fit) * "/"
    fig = _plot_noisecurve_fit(fitres; title = plt_header, subtitle = _noise_fit_subtitle(fitres))
    pname = plt_folder * _get_pltfilename(data, filekey, channel,
                                          Symbol("noisecurve_fit_rc_$(waveform_type)_pickoff"))
    save(pname, fig)
    @info "Save noise-model fit plot to $pname"

    return fitres
end
export process_noisecurve_fit
