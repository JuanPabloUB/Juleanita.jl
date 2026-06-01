function _local_peak_amplitude(signal)
    i = argmax(signal)
    lo = max(firstindex(signal), i - 1)
    hi = min(lastindex(signal), i + 1)
    mean(view(signal, lo:hi))
end

# Take the first `n_evts` waveforms, clamped to the actual length. If `n_evts`
# is non-finite (e.g. `NaN`), return the full set unchanged.
function _take_n_evts(wvfs, n_evts)
    isfinite(n_evts) || return wvfs
    return wvfs[1:min(Int(n_evts), length(wvfs))]
end

"""
    dsp_rc_rt_optimization(wvfs::ArrayOfRDWaveforms, config::DSPConfig, τ::Quantity{T}) where T<:Real

Build an RC optimization grid by filtering each waveform with `RCFilter(rc = rc)`
for every value in the RC scan grid and taking the local peak amplitude of the
filtered signal.

This mirrors the pickoff-style optimization helpers used for trap/cusp/zac, but
uses the filtered waveform peak instead of a fixed pickoff time.
"""
function dsp_rc_rt_optimization(wvfs::ArrayOfRDWaveforms, config::DSPConfig, τ::Quantity{T}) where T<:Real
    bl_window = config.bl_window
    rc_grid = config.e_grid_rt_trap

    # Shift all waveforms to a common baseline before the RC scan.
    bl_stats = signalstats.(wvfs, leftendpoint(bl_window), rightendpoint(bl_window))
    wvfs = shift_waveform.(wvfs, -bl_stats.mean)

    # Apply the same pole-zero preprocessing used elsewhere in filter optimization.
    if τ > 0.0u"µs"
        wvfs = InvCRFilter(τ).(wvfs)
    end

    enc_rc_grid = zeros(Float64, length(rc_grid), length(wvfs))
    for (r, rc) in enumerate(rc_grid)
        wvfs_flt = RCFilter(rc = rc).(wvfs)
        enc_rc_grid[r, :] = _local_peak_amplitude.(wvfs_flt.signal)
    end

    return enc_rc_grid
end
export dsp_rc_rt_optimization

"""
    _rc_noise_and_pickoff(wvfs, dsp_config, τ_pz)

Single-pass RC sweep that produces both:

- baseline-window RMS noise per RC (for the noise curve), and
- per-event peak amplitudes per RC (`amp_grid`, for the µ_pickoff Gaussian
  fit that anchors the V_ref-based keV calibration).

Merges what `noise_sweep(:rc, …)` and `dsp_rc_rt_optimization` would each do
separately — the expensive per-RC `RCFilter` pass is shared, so `:bl_noise`
mode runs about 2× faster than calling both helpers in sequence.

Returns a NamedTuple `(rc_opt, min_noise, rc, noise, f_interp, amp_grid)`,
restricted to the RC values where the baseline RMS is finite.
"""
function _rc_noise_and_pickoff(wvfs::ArrayOfRDWaveforms, dsp_config::DSPConfig, τ_pz::Quantity)
    bl_window = dsp_config.bl_window
    rc_grid_q = collect(dsp_config.e_grid_rt_trap)

    bl_stats = signalstats.(wvfs, leftendpoint(bl_window), rightendpoint(bl_window))
    wvfs_shift = shift_waveform.(wvfs, -bl_stats.mean)
    wvfs_pz = τ_pz > 0.0u"µs" ? InvCRFilter(τ_pz).(wvfs_shift) : wvfs_shift

    noise = zeros(length(rc_grid_q))
    amp_grid = zeros(Float64, length(rc_grid_q), length(wvfs_pz))

    for (i, rc) in enumerate(rc_grid_q)
        wvfs_flt = RCFilter(rc = rc).(wvfs_pz)
        valid_bins = findall(leftendpoint(bl_window) .<= wvfs_flt[1].time .<= rightendpoint(bl_window))
        if !isempty(valid_bins)
            bl_samples = filter.(isfinite, map(x -> x.signal[valid_bins], wvfs_flt))
            noise[i] = rms(vcat(bl_samples...))
        else
            noise[i] = NaN
        end
        amp_grid[i, :] = _local_peak_amplitude.(wvfs_flt.signal)
    end

    any(isfinite.(noise)) ||
        error("no finite noise values found in RC sweep — try adjusting the RC grid")

    finite_idx = findall(isfinite.(noise))
    rc_finite_q = rc_grid_q[finite_idx]
    rc_finite_us = ustrip.(rc_finite_q)
    noise_finite = noise[finite_idx]
    amp_grid_finite = amp_grid[finite_idx, :]

    f_interp = if length(noise_finite) >= 4
        BSinterpolate(rc_finite_us, noise_finite, BSplineOrder(4))
    elseif length(noise_finite) >= 2
        LinearInterpolation(rc_finite_us, noise_finite)
    else
        x -> NaN
    end

    opt_result = optimize(f_interp, minimum(rc_finite_us), maximum(rc_finite_us))
    rc_opt = Optim.minimizer(opt_result) * u"µs"
    min_noise = Optim.minimum(opt_result)

    return (
        rc_opt    = rc_opt,
        min_noise = min_noise,
        rc        = rc_finite_q,
        noise     = noise_finite,
        f_interp  = f_interp,
        amp_grid  = amp_grid_finite,
    )
end

function process_noisecurve end
export process_noisecurve
function process_noisecurve(data::LegendData, period::DataPeriod, run::DataRun, category::Union{Symbol, DataCategory}, channel::ChannelId, dsp_config::DSPConfig; reprocess::Bool = false, waveform_type::Symbol = :waveform, n_evts::Union{<:Float64, <:Int} = NaN, τ_pz = 0.0u"µs", rt_opt_mode::Symbol = :bl_noise, qmin::Real = 0.02, qmax::Real = 0.98, nbins::Union{Nothing, Int} = nothing, rel_cut_fit::Real = 0.1, diagnostics::Bool = true)
    qmax > qmin || throw(ArgumentError("qmax must be larger than qmin"))
    rt_opt_mode in (:bl_noise, :pickoff) || error("Unsupported rt_opt_mode $rt_opt_mode. Use :bl_noise or :pickoff.")

    filekeys = search_disk(FileKey, data.tier[DataTier(:raw), category , period, run])
    isempty(filekeys) && error("No raw filekeys found for $category/$period/$run")
    filekey = first(filekeys)
    mode_label = rt_opt_mode == :bl_noise ? :blnoise : rt_opt_mode
    sweep_key = Symbol("rc_$(waveform_type)_$(mode_label)")

    # load or calculate RC noise curve
    pars_pd = if isfile(joinpath(data_path(data.par[category].rpars.noise[period]), "$run.json" )) && !reprocess
        @info "Load RC noise curve results for $category-$period-$run-$channel"
        data.par[category].rpars.noise[period,run, channel]
    else
        PropDict()
    end

    # `data_raw` may be loaded by the fresh-compute branch and reused later
    # for the :bl_noise pulser sweep, or it may stay nothing if the cache is hit.
    data_raw = nothing

    if !haskey(pars_pd, sweep_key)
        data_raw = read_ldata(data, DataTier(:raw), filekeys, channel)
        wvfs = _take_n_evts(getproperty(data_raw, waveform_type), n_evts)

        if rt_opt_mode == :bl_noise
            # Single-pass: baseline-RMS noise and pickoff-amplitude grid in one sweep.
            sweep = _rc_noise_and_pickoff(wvfs, dsp_config, τ_pz)

            amp_min, amp_max = _quantile_truncfit(sweep.amp_grid; qmin = qmin, qmax = qmax)
            nbins_fit_pickoff = isnothing(nbins) ? round(Int, size(sweep.amp_grid, 2) / 5) : nbins
            µ_fit_result, _ = _fit_rc_gaussian_fits(sweep.amp_grid, sweep.rc,
                                                    amp_min, amp_max, nbins_fit_pickoff, rel_cut_fit)
            all(µ_fit_result.fit_success) ||
                error("Not all pickoff Gaussian fits succeeded — cannot derive µ_pickoff for keV scale")

            result_rc = (
                rc_opt    = sweep.rc_opt,
                min_noise = sweep.min_noise,
                rc        = sweep.rc,
                noise     = sweep.noise,
                µ_pickoff = µ_fit_result.μ,
                τ_pz      = τ_pz,
            )
            report_rc = merge(result_rc, (f_interp = sweep.f_interp,))
        else  # :pickoff
            enc_grid = dsp_rc_rt_optimization(wvfs, dsp_config, τ_pz)
            enc_min, enc_max = _quantile_truncfit(enc_grid; qmin = qmin, qmax = qmax)
            nbins_fit = isnothing(nbins) ? round(Int, size(enc_grid)[2] / 5) : nbins
            result_rt, report_rt = _fit_rc_gaussian_fits(enc_grid, dsp_config.e_grid_rt_trap, enc_min, enc_max, nbins_fit, rel_cut_fit)

            if diagnostics
                det = _channel2detector(data, channel)
                plt_folder = LegendDataManagement.LDMUtils.get_pltfolder(data, filekey, Symbol("noisecurve_rc_$(waveform_type)_pickoff_fits")) * "/"
                for (rc, report_fit) in zip(report_rt.rc, report_rt.fit_reports)
                    fig = Figure()
                    LegendMakie.lplot!(
                        report_fit,
                        figsize = (600, 430),
                        titlesize = 17,
                        title = get_plottitle(filekey, det, "RC Pickoff Distribution") *
                                @sprintf("\nrc = %.2f %s", ustrip(rc), unit(rc)),
                        juleana_logo = false,
                        xlabel = "CSA output amplitude (ADC)",
                    )
                    rc_label = replace(@sprintf("%.2f", ustrip(rc)), "." => "p")
                    pname = plt_folder * _get_pltfilename(data, filekey, channel, Symbol("noisecurve_rc_$(waveform_type)_pickoff_fit_$(rc_label)"))
                    save(pname, fig)
                    @info "Save RC pickoff fit plot to $pname"
                end
            end

            result_rc = (
                rc_opt      = result_rt.rc_min_sigma,
                min_enc     = result_rt.min_sigma,
                rc          = result_rt.rc,
                enc         = result_rt.σ,
                µ_pickoff   = result_rt.μ,
                fit_success = result_rt.fit_success,
                qmin = qmin, qmax = qmax,
                nbins = nbins_fit, rel_cut_fit = rel_cut_fit,
                min_amp = enc_min, max_amp = enc_max,
                τ_pz = τ_pz,
            )
            report_rc = result_rc
        end

        pars_pd = merge(pars_pd, Dict(sweep_key => result_rc))
        writelprops(data.par[category].rpars.noise[period], run, PropDict("$channel" => pars_pd))
        @info "Save RC noise curve results to pars (type $waveform_type, mode $rt_opt_mode)"
    else
        result_rc = data.par[category].rpars.noise[period, run, channel][sweep_key]
        if rt_opt_mode == :bl_noise
            f_interp = let enc = result_rc.noise, rc = ustrip.(result_rc.rc)
                if length(enc) >= 4
                    BSinterpolate(rc, enc, BSplineOrder(4))
                else
                    LinearInterpolation(rc, enc)
                end
            end
            report_rc = merge(result_rc, PropDict(:f_interp => f_interp))
        else  # :pickoff
            report_rc = result_rc
        end
    end

    # Pulser pairing — only :bl_noise actually needs the pulser waveforms;
    # :pickoff just reads the σ_adc from the par tier.
    if rt_opt_mode == :bl_noise
        if isnothing(data_raw)
            data_raw = read_ldata(data, DataTier(:raw), filekeys, channel)
        end
        pulser_wvfs = _take_n_evts(getproperty(data_raw, :pulser), n_evts)
        _, pulser_report_rt = noise_sweep(:rc, pulser_wvfs, dsp_config, 0.0u"µs")
        _rc_grids_match(report_rc.rc, pulser_report_rt.rt) ||
            error("RC grid mismatch between waveform and pulser baseline-noise curves " *
                  "(likely a failed Gaussian fit at one or more RC points).")
        report_rc = merge(report_rc, PropDict(:pulser_noise => pulser_report_rt.noise))
    else  # :pickoff
        pulser_cal = data.par[category].rpars.ecal[period, run, channel][:pulser_vcal_rc_curve]
        _rc_grids_match(report_rc.rc, pulser_cal.rc) ||
            error("RC grid mismatch between noisecurve and pulser calibration " *
                  "(likely a failed Gaussian fit at one or more RC points).")
        report_rc = merge(report_rc, PropDict(
            :pulser_fwhm_adc     => 2.355 .* pulser_cal.σ_adc,
            :pulser_fwhm_adc_err => 2.355 .* pulser_cal.σ_adc_err,
        ))
    end

    # Hardware metadata (read once, used for the title AND the in-situ V_ref calibration).
    hw_meta = getproperty(data.metadata.hardware, Symbol(data.name))(filekey)
    hasproperty(hw_meta, :injection_voltage) || error("hardware config does not contain injection_voltage")
    hasproperty(hw_meta, :V_ref) || error("hardware config does not contain V_ref")
    hasproperty(hw_meta, :V_DD) || error("hardware config does not contain V_DD")
    injection_voltage = uconvert(u"V", hw_meta.injection_voltage)
    V_ref             = uconvert(u"V", hw_meta.V_ref)
    V_DD              = uconvert(u"V", hw_meta.V_DD)
    gain              = hw_meta.gain_tot
    cap_feedback_F    = ustrip(u"F", hw_meta.cap_feedback)

    # Operating temperature for the Ge W_eh(T) lookup — taken from hardware metadata only.
    # NB: Ge_Energy_per_eholePair is calibrated for cryogenic operation (≈77–120 K); at
    # room temperature it extrapolates without physical meaning.
    hasproperty(hw_meta, :operating_temperature) || error("hardware config does not contain operating_temperature")
    temp_K = ustrip(u"K", hw_meta.operating_temperature)
    W_eh = Ge_Energy_per_eholePair(temp_K)

    # In-situ ADC → V at DAQ input, per RC (computed once, used for all y-units).
    µ_pickoff    = mvalue.(report_rc.µ_pickoff)
    length(µ_pickoff) == length(report_rc.rc) ||
        error("µ_pickoff (n=$(length(µ_pickoff))) / report.rc (n=$(length(report_rc.rc))) length mismatch")
    adc_to_V_DAQ = ustrip(u"V", V_ref) ./ µ_pickoff

    # LaTeX-styled hardware tag: every X_xx identifier becomes math-mode with an
    # upright multi-character subscript (e.g. V_inj → $V_\mathrm{inj}$). Rendered
    # by MathTeXEngine; passed as the Axis subtitle so it occupies its own line
    # while leaving the plain header in Makie's default sans-serif font.
    title_hw = Makie.LaTeXString(@sprintf(
        "\$C_f\$ = %.0f fF, \$C_\\mathrm{inj}\$ = %.0f fF, \$V_\\mathrm{inj}\$ = %.3f V, T = %d K, \$V_\\mathrm{DD}\$ = %.3f V",
        ustrip(u"fF", hw_meta.cap_feedback),
        ustrip(u"fF", hw_meta.cap_inj),
        ustrip(u"V", injection_voltage),
        temp_K,
        ustrip(u"V", V_DD),
    ))
    plot_title = rt_opt_mode == :bl_noise ? "RC noise curve" : "RC pickoff curve"
    plt_header = get_plottitle(filekey, _channel2detector(data, channel), plot_title)
    plt_folder = LegendDataManagement.LDMUtils.get_pltfolder(data, filekey, :noisecurve) * "/"

    for yunit in [:ADC, :e, :keV]
        plt = if rt_opt_mode == :bl_noise
            plot_noisecurve(report_rc, yunit, adc_to_V_DAQ;
                            gain = gain, cap_feedback = cap_feedback_F, W_eh = W_eh,
                            title = plt_header, subtitle = title_hw)
        else
            plot_noisecurve_pickoff(report_rc, yunit, adc_to_V_DAQ;
                                    gain = gain, cap_feedback = cap_feedback_F, W_eh = W_eh,
                                    title = plt_header, subtitle = title_hw)
        end
        pname = plt_folder * _get_pltfilename(data, filekey, channel, Symbol("noisecurve_rc_$(waveform_type)_$(mode_label)_$(plt.yunit)"))
        save(pname, plt.fig)
        @info "Save plot to $pname"
    end

    return result_rc, report_rc
end

# Compare two RC grids that should be aligned, with a small tolerance on the
# stripped µs values to avoid spurious mismatches from Quantity equality on
# floating-point step ranges.
function _rc_grids_match(rc_a, rc_b)
    length(rc_a) == length(rc_b) || return false
    a_us = ustrip.(u"µs", rc_a)
    b_us = ustrip.(u"µs", rc_b)
    all(isapprox.(a_us, b_us; atol = 1e-9, rtol = 1e-9))
end

"""
    _noisecurve_yscale(adc_to_V_DAQ, yunit; gain, cap_feedback, W_eh)

Pure unit transformation. Given a precomputed per-RC `adc_to_V_DAQ` vector
(V at the DAQ input per ADC count) and target `yunit`, return the per-RC
scalar factor `y_scale[i]` such that `σ_ADC[i] × y_scale[i]` is `σ` in the
requested unit. `gain` is the buffer-board gain (used to back-out from
V_DAQ to V_CSA before charge/energy conversion), `cap_feedback` is the CSA
feedback capacitance in F, and `W_eh` is the Ge mean energy per
electron-hole pair in eV at the detector temperature.

For `yunit == :keV` the chain is:
`σ_ADC × adc_to_V_DAQ / gain × C_F / e × W_eh × 10⁻³`.
"""
function _noisecurve_yscale(adc_to_V_DAQ::AbstractVector, yunit::Symbol;
                            gain::Real, cap_feedback::Real, W_eh::Real)
    if yunit == :ADC
        ones(length(adc_to_V_DAQ))
    elseif yunit == :V
        adc_to_V_DAQ
    elseif yunit == :e
        adc_to_V_DAQ ./ gain .* cap_feedback ./ electron_charge
    elseif yunit == :keV
        adc_to_V_DAQ ./ gain .* cap_feedback ./ electron_charge .* W_eh ./ 1e3
    else
        error("Invalid yunit: $yunit")
    end
end



function plot_noisecurve(report, yunit::Symbol, adc_to_V_DAQ::AbstractVector;
                         gain::Real, cap_feedback::Real, W_eh::Real, title = "", subtitle = "")
    y_scale = _noisecurve_yscale(adc_to_V_DAQ, yunit; gain = gain, cap_feedback = cap_feedback, W_eh = W_eh)
    x = report.rc
    x_unit = unit(x[1])
    x = ustrip.(x)
    y = report.noise .* y_scale
    y_pulser = report.pulser_noise .* y_scale

    fig = Figure()
    ax = Axis(fig[1, 1],
        title = title, subtitle = subtitle,
        xlabel = "RC time constant ($x_unit)", ylabel = "Noise ($yunit)",
        limits = ((extrema(x)[1] - 0.2, extrema(x)[2] + 0.2), (nothing, nothing)))
    lines!(ax, x, y, color = (:blue, 0.5), linewidth = 3, linestyle = :solid, label = nothing)
    Makie.scatter!(ax, x, y,  color = :blue, label = "CSA output RMS")
    lines!(ax, x, y_pulser, color = (:red, 0.5), linewidth = 3, linestyle = :dash, label = nothing)
    Makie.scatter!(ax, x, y_pulser, color = :red, label = "Pulser RMS")
    axislegend()
    return (fig = fig, ax = ax, y_scale = y_scale, yunit = yunit)
end
export plot_noisecurve

function plot_noisecurve_pickoff(report, yunit::Symbol, adc_to_V_DAQ::AbstractVector;
                                  gain::Real, cap_feedback::Real, W_eh::Real, title = "", subtitle = "")
    y_scale = _noisecurve_yscale(adc_to_V_DAQ, yunit; gain = gain, cap_feedback = cap_feedback, W_eh = W_eh)
    x = report.rc
    x_unit = unit(x[1])
    x = ustrip.(x)
    y = 2.355 .* mvalue.(report.enc) .* y_scale
    y_err = 2.355 .* muncert.(report.enc) .* y_scale
    y_pulser = report.pulser_fwhm_adc .* y_scale
    y_pulser_err = report.pulser_fwhm_adc_err .* y_scale
    # Intrinsic CSA noise = total minus DAQ floor, in quadrature.
    # max(·, 0) clamps any noise-correlation artefact that would otherwise produce a
    # negative under-the-root (typically a sign of common-mode pickup correlating
    # both channels — diagnostic rather than physical).
    y_intrinsic = sqrt.(max.(y .^ 2 .- y_pulser .^ 2, 0))

    fig = Figure(size = (900, 500))
    ax = Axis(fig[1, 1],
        title = title, subtitle = subtitle,
        xlabel = "RC time constant ($x_unit)", ylabel = "ENC ($yunit)",
        limits = ((extrema(x)[1] - 0.2, extrema(x)[2] + 0.2), (nothing, nothing)))
    lines!(ax, x, y, color = (:blue, 0.5), linewidth = 3, linestyle = :solid, label = nothing)
    Makie.errorbars!(ax, x, y, y_err, color = (:blue, 0.5))
    Makie.scatter!(ax, x, y, color = :blue, label = "CSA output FWHM")
    lines!(ax, x, y_pulser, color = (:red, 0.5), linewidth = 3, linestyle = :dash, label = nothing)
    Makie.errorbars!(ax, x, y_pulser, y_pulser_err, color = (:red, 0.5))
    Makie.scatter!(ax, x, y_pulser, color = :red, label = "Pulser FWHM")
    lines!(ax, x, y_intrinsic, color = (:green, 0.5), linewidth = 3, linestyle = :dashdot, label = nothing)
    Makie.scatter!(ax, x, y_intrinsic, color = :green, label = "CSA FWHM intrinsic")
    Legend(fig[1, 2], ax)
    return (fig = fig, ax = ax, y_scale = y_scale, yunit = yunit)
end
export plot_noisecurve_pickoff
