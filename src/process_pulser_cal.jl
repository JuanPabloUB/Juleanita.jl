"""
    _pulser_voltage_V(hardware_config)

Extract the pulser voltage from the hardware config and return it as a
`Unitful` voltage.
"""
function _pulser_voltage_V(hardware_config)
    hasproperty(hardware_config, :pulser_voltage) || error("hardware_config does not contain pulser_voltage")
    uconvert(u"V", getproperty(hardware_config, :pulser_voltage))
end

"""
    calibrate_from_pulser(data::LegendData, period::DataPeriod, run::DataRun,
                          category::Union{Symbol, DataCategory}, channel::ChannelId,
                          hardware_config, dsp_config::DSPConfig; kwargs...)

Build an RC-dependent ADC-to-pulser-voltage calibration curve from the pulser
waveforms in the raw tier.

Strategy:
- load `raw/pulser`
- subtract the baseline using `dsp_config.bl_window`
- for each `rc` in `dsp_config.e_grid_rt_trap`:
  - apply `RCFilter(rc = rc)`
  - define pulser amplitude as the maximum of the filtered waveform
  - histogram and fit a truncated Gaussian
  - calculate `adc_to_pulser_V(rc) = pulser_voltage / μ_adc(rc)`

The result is saved under the existing `rpars.ecal` tree using the key
`:pulser_vcal_rc_curve`, so no extra metadata path is needed.
"""
function calibrate_from_pulser end
export calibrate_from_pulser

function calibrate_from_pulser(
    data::LegendData,
    period::DataPeriod,
    run::DataRun,
    category::Union{Symbol, DataCategory},
    channel::ChannelId,
    hardware_config,
    dsp_config::DSPConfig;
    reprocess::Bool = false,
    waveform_type::Symbol = :pulser,
    n_evts::Union{<:Float64, <:Int} = NaN,
    nbins::Int = 50,
    rel_cut_fit::T = 0.1,
    qmin::T = 0.0,
    qmax::T = 1.0,
    diagnostics::Bool = true,
    save_fit_plots::Union{Nothing, Bool} = nothing,
) where {T <: Real}
    qmax > qmin || throw(ArgumentError("qmax must be larger than qmin"))
    diagnostics = isnothing(save_fit_plots) ? diagnostics : save_fit_plots

    filekeys = search_disk(FileKey, data.tier[DataTier(:raw), category, period, run])
    isempty(filekeys) && throw(ArgumentError("No raw files found for $category-$period-$run"))
    filekey = filekeys[1]
    det = _channel2detector(data, channel)

    sweep_key = :pulser_vcal_rc_curve

    pars_pd = if isfile(joinpath(data_path(data.par[category].rpars.ecal[period]), "$run.json")) && !reprocess
        @info "Load pulser calibration results for $category-$period-$run-$channel"
        data.par[category].rpars.ecal[period, run, channel]
    else
        PropDict()
    end

    if !haskey(pars_pd, sweep_key)
        data_raw = read_ldata(data, DataTier(:raw), filekeys, channel)
        hasproperty(data_raw, waveform_type) || error("raw data does not contain $(waveform_type) waveforms")

        wvfs = if isfinite(n_evts)
            n_evts = n_evts < length(getproperty(data_raw, waveform_type)) ? n_evts : length(getproperty(data_raw, waveform_type))
            getproperty(data_raw, waveform_type)[1:n_evts]
        else
            getproperty(data_raw, waveform_type)
        end

        bl_stats = signalstats.(wvfs, leftendpoint(dsp_config.bl_window), rightendpoint(dsp_config.bl_window))
        wvfs_bl = shift_waveform.(wvfs, -bl_stats.mean)

        rc_grid = collect(dsp_config.e_grid_rt_trap)
        pulser_voltage = _pulser_voltage_V(hardware_config)

        μ_adc = fill(NaN, length(rc_grid))
        μ_adc_err = fill(NaN, length(rc_grid))
        σ_adc = fill(NaN, length(rc_grid))
        σ_adc_err = fill(NaN, length(rc_grid))
        adc_to_pulser_V = fill(NaN, length(rc_grid))
        adc_to_pulser_V_err = fill(NaN, length(rc_grid))
        fit_success = falses(length(rc_grid))

        amp_grid = fill(NaN, length(rc_grid), length(wvfs_bl))
        for (i, rc) in enumerate(rc_grid)
            try
                wvfs_rc = RCFilter(rc = rc).(wvfs_bl)
                amp_grid[i, :] = _local_peak_amplitude.(wvfs_rc.signal)
            catch e
                @warn "Pulser calibration amplitude extraction failed for rc = $rc" exception = (e, catch_backtrace())
            end
        end

        amp_min, amp_max = _quantile_truncfit(amp_grid; qmin = qmin, qmax = qmax)
        fit_result, fit_report = _fit_rc_gaussian_fits(amp_grid, rc_grid, amp_min, amp_max, nbins, rel_cut_fit)
        fit_success = fit_result.fit_success

        plt_folder = LegendDataManagement.LDMUtils.get_pltfolder(data, filekey, :pulsercal) * "/"
        for (j, i) in enumerate(fit_report.fit_indices)
            rc = rc_grid[i]
            μ_fit = fit_result.μ[j]
            σ_fit = fit_result.σ[j]

            μ_adc[i] = mvalue(μ_fit)
            μ_adc_err[i] = muncert(μ_fit)
            σ_adc[i] = mvalue(σ_fit)
            σ_adc_err[i] = muncert(σ_fit)

            adc_cal = pulser_voltage / μ_fit
            adc_to_pulser_V[i] = ustrip(u"V", mvalue(adc_cal))
            adc_to_pulser_V_err[i] = ustrip(u"V", muncert(adc_cal))

            if diagnostics
                fig = Figure()
                LegendMakie.lplot!(
                    fit_report.fit_reports[j],
                    figsize = (600, 430),
                    titlesize = 17,
                    title = get_plottitle(filekey, det, "Pulser RC Distribution") *
                            @sprintf("\nrc = %.2f %s, pulser = %.3f %s", ustrip(rc), unit(rc), ustrip(pulser_voltage), unit(pulser_voltage)),
                    juleana_logo = false,
                    xlabel = "Pulser amplitude (ADC)",
                )
                rc_label = replace(@sprintf("%.2f", ustrip(rc)), "." => "p")
                pname = plt_folder * _get_pltfilename(data, filekey, channel, Symbol("pulser_cal_rcfit_$(rc_label)"))
                save(pname, fig)
                @info "Save pulser calibration fit plot to $pname"
            end
        end

        result_cal = (
            pulser_voltage = pulser_voltage,
            rc = rc_grid,
            μ_adc = μ_adc,
            μ_adc_err = μ_adc_err,
            σ_adc = σ_adc,
            σ_adc_err = σ_adc_err,
            adc_to_pulser_V = adc_to_pulser_V .* u"V",
            adc_to_pulser_V_err = adc_to_pulser_V_err .* u"V",
            fit_success = fit_success,
            waveform_type = waveform_type,
            qmin = qmin,
            qmax = qmax,
            nbins = nbins,
            rel_cut_fit = rel_cut_fit,
            min_amp = amp_min,
            max_amp = amp_max,
        )

        pars_pd = merge(pars_pd, Dict(sweep_key => result_cal))
        writelprops(data.par[category].rpars.ecal[period], run, PropDict("$channel" => pars_pd))
        @info "Saved pulser RC calibration curve to pars"
    else
        result_cal = data.par[category].rpars.ecal[period, run, channel][sweep_key]
    end

    report_cal = result_cal
    return result_cal, report_cal
end

process_pulser_cal(args...; kwargs...) = calibrate_from_pulser(args...; kwargs...)
export process_pulser_cal
