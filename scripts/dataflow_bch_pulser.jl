using Juleanita
using LegendDataManagement
using LegendDataManagement: LDMUtils
using LegendHDF5IO
using LegendDSP
using Measurements
using Unitful
using Dates

ENV["LEGEND_DATA_CONFIG"] = "/ptmp/jubeteta/juleanita/mpik/asic/config.json"

# inputs / settings 
asic = LegendData(:asic)
period = DataPeriod(1)
run = DataRun(9)
channel = ChannelId(1)
category = :bch
timestep = 0.01u"µs"
filter_types = [:trap]
reprocess = true
diagnostics = true


#use today date for individual timestamps
#tstamp = round(Int, datetime2unix(DateTime(2026, 5, 6, 17, 39, 17)))
# 0. raw conversion
#caen_bin_to_lh5(asic, period, run, category, channel; timestep=timestep)

# 1. load configs
filekeys   = search_disk(FileKey, asic.tier[DataTier(:raw), category, period, run])
isempty(filekeys) && error("No raw filekeys found for $category/$period/$run")
@info "Using first raw filekey for validity selection" first_filekey=filekeys[1]
dsp_config = DSPConfig(dataprod_config(asic).dsp(filekeys[1]).default)
pz_config  = dataprod_config(asic).dsp(filekeys[1]).pz.default
hardware_config = asic.metadata.hardware.asic(filekeys[1])

# 2. decay time  (SKIPPED for fit-only — cached, and τ_pz is unused by the fit)
#plot_τ = process_decaytime(asic, period, run, category, channel, pz_config, dsp_config; reprocess=reprocess)
#τ_pz = asic.par[category].rpars.pz[period, run, channel].τ

# Shared fit tuning so the pulser cal and the noise-curve pickoff use the
# exact same Gaussian-fit recipe (only the physical input differs).
# qmin/qmax must be wide enough that the histogram range extends past the
# rel_cut_fit threshold, i.e. quantile range ≥ ±√(-2 ln rel_cut_fit) σ.
# For rel_cut_fit = 0.05 that means qmin ≤ 0.007 — using 0.005/0.995 here gives
# ±2.58 σ histogram range, comfortably past the ±2.45 σ crossing point.
#fit_kwargs = (; rel_cut_fit = 0.05, nbins = 100, qmin = 0.005, qmax = 0.995)

# (SKIPPED for fit-only — pulser cal already cached in rpars.ecal)
#result_cal, report_cal = process_pulser_cal(asic, period, run, category, channel, hardware_config, dsp_config;
#                reprocess = true, waveform_type = :pulser, diagnostics = diagnostics, fit_kwargs...)

# 3.5 RC noise curve  (SKIPPED for fit-only — pickoff curve already cached in rpars.noise)
#result_rc, report_rc = process_noisecurve(asic, period, run, category, channel, dsp_config;
#                reprocess = true, τ_pz = mvalue(τ_pz), rt_opt_mode = :pickoff,
#                diagnostics = diagnostics, fit_kwargs...)


# 3.6 CSA noise-model fit on the intrinsic FWHM curve → rpars.noise_fit + plot
# All hardware parameters (Rf + 5 % error, cap_input_residual = 0) are read
# from metadata. C_in = cap_feedback + cap_inj + cap_input_residual = 600 fF.
fitres = process_noisecurve_fit(asic, period, run, category, channel, dsp_config;
                reprocess = true)
@info "CSA noise params" vn=fitres.vn IL=fitres.IL f1f=fitres.f1f χ²_dof=fitres.χ²_dof



# 3. filter optimizaiton 
#process_filteropt(asic, period, run, category, channel, dsp_config, mvalue(τ_pz), :all; 
#                reprocess = true, rt_opt_mode = :pickoff, filter_types = filter_types, fwhm_rel_cut_fit = 0.005)
#pars_filter = asic.par[category].rpars.fltopt[period, run, channel]jk
# 3. run dsp on all waveforms in raw tier. output: dsp files
#process_dsp(asic, period, run, category, channel, dsp_config, mvalue(τ_pz), pars_filter; reprocess = reprocess)
#dsp_pars = read_ldata(asic, :jldsp, category, period, run, channel);

# 4. fit peaks 
#process_peakfits(asic, period, run, category, channel; reprocess = true, juleana_logo = true, rel_cut_fit = 0.1)

# 5. linearity fit and plot
#result, report = process_pulser_linearity(asic, period, collect(1:10), category, channel)
#result
#lplot(report; plot_gof = false)
