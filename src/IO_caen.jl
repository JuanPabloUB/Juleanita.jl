"""
    _read_caen_bin(bin_file::AbstractString)

Parse a CAEN binary waveform file and return the event fields as vectors.
"""
function _read_caen_bin(bin_file::AbstractString)
    open(bin_file, "r") do io
        header_bytes = read(io, 2)
        n_header = header_bytes == b"\xe9\xca" ? 2 : 0
        seek(io, n_header)

        boards = UInt16[]
        channels = UInt16[]
        timestamps = UInt64[]
        energies = UInt16[]
        flags = UInt32[]
        wavecodes = UInt8[]
        n_samples = UInt32[]
        waveforms = Vector{Vector{Int32}}()

        while !eof(io)
            push!(boards, read(io, UInt16))
            push!(channels, read(io, UInt16))
            push!(timestamps, read(io, UInt64))
            push!(energies, read(io, UInt16))
            push!(flags, read(io, UInt32))
            push!(wavecodes, read(io, UInt8))

            n_samples_evt = read(io, UInt32)
            push!(n_samples, n_samples_evt)
            samples = Vector{Int16}(undef, Int(n_samples_evt))
            read!(io, samples)
            push!(waveforms, Int32.(samples))
        end

        return (; boards, channels, timestamps, energies, flags, wavecodes, n_samples, waveforms)
    end
end

"""
    _find_caen_bin_files(bin_folder::AbstractString)

Find CAEN binary files in a bin-tier folder.
"""
function _find_caen_bin_files(bin_folder::AbstractString)
    isdir(bin_folder) || throw(ArgumentError("CAEN bin tier folder not found: $bin_folder"))

    bin_files = sort(filter(path -> isfile(path) && endswith(lowercase(path), ".bin"), readdir(bin_folder, join = true)))
    isempty(bin_files) && throw(ArgumentError("No CAEN binary files found in $bin_folder"))

    return bin_files
end

"""
    _caen_single_channel_id(parsed)

Return the unique channel id stored in a parsed CAEN binary file.
"""
function _caen_single_channel_id(parsed)
    isempty(parsed.channels) && throw(ArgumentError("CAEN binary contains no channel entries"))
    chs = unique(parsed.channels)
    length(chs) == 1 || throw(ArgumentError("Expected one unique channel id per CAEN binary file, found $(collect(chs))"))
    return only(chs)
end

"""
    _assign_caen_waveform_and_pulser(bin_file_a::AbstractString, bin_file_b::AbstractString)

Read two CAEN binary files and assign the lower channel id to the waveform
stream and the higher channel id to the pulser stream.
"""
function _assign_caen_waveform_and_pulser(bin_file_a::AbstractString, bin_file_b::AbstractString)
    parsed_a = _read_caen_bin(bin_file_a)
    parsed_b = _read_caen_bin(bin_file_b)

    ch_a = _caen_single_channel_id(parsed_a)
    ch_b = _caen_single_channel_id(parsed_b)

    ch_a == ch_b && throw(ArgumentError("Both CAEN files have the same channel id ($ch_a); cannot infer waveform/pulser roles"))

    if ch_a < ch_b
        return (waveform_bin = bin_file_a, pulser_bin = bin_file_b, parsed_waveform = parsed_a, parsed_pulser = parsed_b)
    else
        return (waveform_bin = bin_file_b, pulser_bin = bin_file_a, parsed_waveform = parsed_b, parsed_pulser = parsed_a)
    end
end

"""
    caen_bin_to_lh5(data::LegendData, period::DataPeriod, run::DataRun, category::Union{Symbol, DataCategory}, channel::ChannelId; tstamp::Int = 0, timestep::Quantity = 0.01u"µs")

Resolve the input bin-tier folder through LegendDataManagement and convert all
CAEN binary files found there into LH5 raw-tier files.
"""
function caen_bin_to_lh5(
    data::LegendData,
    period::DataPeriod,
    run::DataRun,
    category::Union{Symbol, DataCategory},
    channel::ChannelId;
    tstamp::Int = 0,
    timestep::Quantity = 0.01u"µs",
)
    bin_folder = data.tier[DataTier(:bin), category, period, run]
    bin_files = _find_caen_bin_files(bin_folder)

    @info "found $(length(bin_files)) CAEN binary file(s) in $bin_folder"
    if length(bin_files) == 1
        return caen_bin_to_lh5(data, period, run, category, channel, bin_files[1]; tstamp = tstamp, timestep = timestep)
    elseif length(bin_files) == 2
        return caen_bin_to_lh5(data, period, run, category, channel, bin_files[1], bin_files[2]; tstamp = tstamp, timestep = timestep)
    else
        throw(ArgumentError("Expected 1 or 2 CAEN binary files in $bin_folder, found $(length(bin_files))"))
    end
end

"""
    caen_bin_to_lh5(data::LegendData, period::DataPeriod, run::DataRun, category::Union{Symbol, DataCategory}, channel::ChannelId, bin_file::AbstractString; tstamp::Int = 0, timestep::Quantity = 0.01u"µs")

Convert a CAEN binary waveform file into an LH5 raw-tier file.
"""
function caen_bin_to_lh5(
    data::LegendData,
    period::DataPeriod,
    run::DataRun,
    category::Union{Symbol, DataCategory},
    channel::ChannelId,
    bin_file::AbstractString;
    tstamp::Int = 0,
    timestep::Quantity = 0.01u"µs",
)
    isfile(bin_file) || throw(ArgumentError("CAEN binary file not found: $bin_file"))

    h5folder = data.tier[DataTier(:raw), category, period, run]
    if !ispath(h5folder)
        mkpath(h5folder)
        @info "created folder: $h5folder"
    end

    parsed = _read_caen_bin(bin_file)
    n_entries = length(parsed.timestamps)
    n_entries > 0 || throw(ArgumentError("CAEN binary file contains no events: $bin_file"))

    timestamp_unix = Int64.(tstamp .+ (parsed.timestamps .÷ UInt64(1_000_000_000_000)))
    wvfs = ArrayOfRDWaveforms([
        RDWaveform(0.0u"µs":timestep:((length(wvf) - 1) * timestep), wvf) for wvf in parsed.waveforms
    ])

    eventnumber = collect(Int64, 1:n_entries)
    daqenergy = Float64.(maximum.(parsed.waveforms) .- minimum.(parsed.waveforms))
    baseline = fill(NaN, n_entries)

    filekey = string(FileKey(data.name, period, run, category, Timestamp(timestamp_unix[1])))
    h5name = joinpath(h5folder, filekey * "-tier_raw.lh5")

    fid = lh5open(h5name, "w")
    try
        fid["$channel/raw/waveform"] = wvfs
        fid["$channel/raw/daqenergy"] = daqenergy
        fid["$channel/raw/eventnumber"] = eventnumber
        fid["$channel/raw/timestamp"] = timestamp_unix
        fid["$channel/raw/baseline"] = baseline
    finally
        close(fid)
    end

    @info "saved $n_entries waveforms in .lh5 file: $h5name"
    return h5name
end

"""
    caen_bin_to_lh5(data::LegendData, period::DataPeriod, run::DataRun, category::Union{Symbol, DataCategory},
                    channel::ChannelId, bin_file_a::AbstractString, bin_file_b::AbstractString;
                    tstamp::Int = 0, timestep::Quantity = 0.01u"µs")

Convert two CAEN binary waveform files into one LH5 raw-tier file. The file
with the lower channel id becomes `raw/waveform`; the other contributes
`raw/pulser`.
"""
function caen_bin_to_lh5(
    data::LegendData,
    period::DataPeriod,
    run::DataRun,
    category::Union{Symbol, DataCategory},
    channel::ChannelId,
    bin_file_a::AbstractString,
    bin_file_b::AbstractString;
    tstamp::Int = 0,
    timestep::Quantity = 0.01u"µs",
)
    isfile(bin_file_a) || throw(ArgumentError("CAEN binary file not found: $bin_file_a"))
    isfile(bin_file_b) || throw(ArgumentError("CAEN binary file not found: $bin_file_b"))

    h5folder = data.tier[DataTier(:raw), category, period, run]
    if !ispath(h5folder)
        mkpath(h5folder)
        @info "created folder: $h5folder"
    end

    assigned = _assign_caen_waveform_and_pulser(bin_file_a, bin_file_b)
    parsed_waveform = assigned.parsed_waveform
    parsed_pulser = assigned.parsed_pulser

    n_entries = length(parsed_waveform.timestamps)
    n_entries > 0 || throw(ArgumentError("CAEN binary file contains no events: $(assigned.waveform_bin)"))
    length(parsed_pulser.timestamps) == n_entries || throw(ArgumentError(
        "Waveform and pulser files have different numbers of events: $(length(parsed_waveform.timestamps)) vs $(length(parsed_pulser.timestamps))"
    ))
    all(parsed_waveform.n_samples .== parsed_pulser.n_samples) || throw(ArgumentError(
        "Waveform and pulser files do not have matching waveform lengths"
    ))

    timestamp_unix = Int64.(tstamp .+ (parsed_waveform.timestamps .÷ UInt64(1_000_000_000_000)))
    wvfs = ArrayOfRDWaveforms([
        RDWaveform(0.0u"µs":timestep:((length(wvf) - 1) * timestep), wvf) for wvf in parsed_waveform.waveforms
    ])
    pulser_wvfs = ArrayOfRDWaveforms([
        RDWaveform(0.0u"µs":timestep:((length(wvf) - 1) * timestep), wvf) for wvf in parsed_pulser.waveforms
    ])

    eventnumber = collect(Int64, 1:n_entries)
    daqenergy = Float64.(maximum.(parsed_waveform.waveforms) .- minimum.(parsed_waveform.waveforms))
    baseline = fill(NaN, n_entries)

    filekey = string(FileKey(data.name, period, run, category, Timestamp(timestamp_unix[1])))
    h5name = joinpath(h5folder, filekey * "-tier_raw.lh5")

    fid = lh5open(h5name, "w")
    try
        fid["$channel/raw/waveform"] = wvfs
        fid["$channel/raw/pulser"] = pulser_wvfs
        fid["$channel/raw/daqenergy"] = daqenergy
        fid["$channel/raw/eventnumber"] = eventnumber
        fid["$channel/raw/timestamp"] = timestamp_unix
        fid["$channel/raw/baseline"] = baseline
    finally
        close(fid)
    end

    @info "saved $n_entries waveform/pulser events in .lh5 file: $h5name"
    return h5name
end
export caen_bin_to_lh5
