function write_test_caen_bin(path::AbstractString; with_header::Bool = true)
    events = [
        (
            board = UInt16(3),
            channel = UInt16(7),
            timestamp = UInt64(2_000_000_000_000),
            energy = UInt16(123),
            flags = UInt32(0x10),
            wavecode = UInt8(1),
            waveform = Int16[10, 11, 12, 13],
        ),
        (
            board = UInt16(4),
            channel = UInt16(8),
            timestamp = UInt64(5_000_000_000_000),
            energy = UInt16(456),
            flags = UInt32(0x20),
            wavecode = UInt8(2),
            waveform = Int16[-2, 0, 2],
        ),
    ]

    open(path, "w") do io
        if with_header
            write(io, b"\xe9\xca")
        end

        for event in events
            write(io, event.board)
            write(io, event.channel)
            write(io, event.timestamp)
            write(io, event.energy)
            write(io, event.flags)
            write(io, event.wavecode)
            write(io, UInt32(length(event.waveform)))
            write(io, event.waveform)
        end
    end

    return events
end

@testset "_read_caen_bin" begin
    mktempdir() do dir
        bin_path = joinpath(dir, "test_caen.bin")
        events = write_test_caen_bin(bin_path)

        parsed = Juleanita._read_caen_bin(bin_path)

        @test parsed.boards == getfield.(events, :board)
        @test parsed.channels == getfield.(events, :channel)
        @test parsed.timestamps == getfield.(events, :timestamp)
        @test parsed.energies == getfield.(events, :energy)
        @test parsed.flags == getfield.(events, :flags)
        @test parsed.wavecodes == getfield.(events, :wavecode)
        @test parsed.n_samples == UInt32[length(event.waveform) for event in events]
        @test parsed.waveforms == [Int32.(event.waveform) for event in events]
    end
end

@testset "_find_caen_bin_files" begin
    mktempdir() do dir
        bin_a = joinpath(dir, "a.BIN")
        bin_b = joinpath(dir, "b.bin")
        txt = joinpath(dir, "ignore.txt")
        mkdir(joinpath(dir, "subdir"))

        write_test_caen_bin(bin_a)
        write_test_caen_bin(bin_b)
        write(txt, "not a CAEN binary")

        @test Juleanita._find_caen_bin_files(dir) == sort([bin_a, bin_b])
        @test_throws ArgumentError Juleanita._find_caen_bin_files(joinpath(dir, "missing"))
    end

    mktempdir() do dir
        @test_throws ArgumentError Juleanita._find_caen_bin_files(dir)
    end
end

@testset "caen_bin_to_lh5 export" begin
    @test isdefined(Juleanita, :caen_bin_to_lh5)
end
