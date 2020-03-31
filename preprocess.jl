using Pkg
Pkg.activate(".")
Pkg.instantiate()
Pkg.resolve()
Pkg.instantiate()
Pkg.resolve()

using Dates, ProgressMeter, CSV, BenchmarkTools, DataFrames
day_to_name = vcat("0" .* string.(1:9), string.(10:31))

function make_url(url_info, date)
    year_str = string(year(date))
    return "http://www.$(first(url_info)).co.uk/archive/$year_str/$(monthname(date))/CSV/" *
        make_filename(url_info, date)
end

function make_filename(url_info, date)
    return string((last(url_info))) *
        day_to_name[day(date)] *
        monthabbr(date) *
        string(year(date)) *
        ".csv"
end

# Specify date range.
start_date = Date(2009, 1, 1)
end_date = Date(2019, 12, 31)
dates = start_date:Day(1):end_date

# Specify urls from which to download data.
url_infos = (
    ("sotonmet", "Sot"),
    ("bramblemet", "Bra"),
    ("chimet", "Chi"),
    ("cambermet", "Cam"),
)



#
# Download the data. This takes a little while.
#

let
    p = Progress(length(url_infos) * length(dates))
    for url_info in url_infos, date in dates
        url = make_url(url_info, date)
        try 
            out = run(`wget -q -P data $url`)
        catch
            out = "failed"
        end
        next!(p; showvalues = [
            (:site, last(url_info)),
            (:date, string(date)),
        ])
    end
end



#
# Compile the data into a single large csv file. This doesn't take very long.
#

let
    N = 288 * length(dates)
    dataset = Matrix{Union{Float64, Missing}}(undef, N, length(url_infos) * 3)
    pos = 0
    for date in dates
        for (p, url_info) in enumerate(url_infos)
            try
                # Load next data file.
                fname = make_filename(url_info, date)
                data = CSV.read(joinpath("data", fname))

                # Compute times and corresponding indices.
                times = in(:TIME, names(data)) ? data[!, :TIME] : data[!, :Time]
                hours = hour.(times) .+ 1
                minute_indices = div.(minute.(times), 5) .+ 1
                time_indices = (hours .- 1) * 12 + minute_indices

                # Pull out relevant data and convert to a matrix.
                useful_data = Matrix(data[!, [:WSPD, :ATMP, :DEPTH]])
                replace!(x-> (x === "N/A") ? missing : x, useful_data)
                for col in 1:size(useful_data, 2)
                    for row in 1:size(useful_data, 1)
                        x = useful_data[row, col]
                        try
                            useful_data[row, col] = x isa Union{Real, Missing} ?
                                x :
                                (x[1:2] == ".-" ?
                                    parse(Float64, "0." * x[3:end]) :
                                    parse(Float64, x))
                        catch e
                            display(e)
                            println()
                        end
                    end
                end

                dataset[pos .+ time_indices, (1:3) .+ (p .- 1) .* 3] = useful_data
            catch e
                if !isa(e, ArgumentError)
                    rethrow(e)
                end
            end
        end
        pos += 288
    end

    # Filter out ridiculous values.
    m, σ = mean(dataset), std(dataset)
    replace!(x -> (x isa Real && (x > m + 5σ || x < m - 5σ)) ? missing : x, dataset)

    # Write results to disk.
    labels = ["wind_speed", "air_temp", "depth"]
    header_names = [Symbol(first(url) * "_" * label) for url in url_infos for label in labels]
    df = DataFrame(dataset, header_names)
    start_time = start_date + Time(0)
    end_time = DateTime(end_date) + Day(1) - Minute(5)
    datetimes = start_time:Minute(5):end_time
    df[!, :date_time] = collect(start_time:Minute(5):end_time)
    CSV.write("output.csv", df)
end



# #
# # Take a look at it to sanity check.
# #

# using Plots
# plotly()

# # Load the data and drop the missings.
# output = CSV.read("output.csv")
# output_sans_missing = dropmissing(output)

# # Compute dataframe without time info.
# names_to_keep = filter(x->x != :date_time, names(output))
# output_sans_time = output[!, names_to_keep]
# date_times = output[!, :date_time]

# plt = plot();
# P, Q = 100_000, 200_000
# for nam in names_to_keep
#     plot!(plt, date_times[P:Q], output[!, nam][P:Q]; label=nam)
# end

# # plt = plot();
# # plot!(plt, date_times[P:Q], output[P:Q, :sotonmet_depth]; label=:sotonmet_depth);
# # plot!(plt, date_times[P:Q], output[P:Q, :bramblemet_depth]; label=:bramblemet_depth);
# # plot!(plt, date_times[P:Q], output[P:Q, :chimet_depth]; label=:chimet_depth);
# # plot!(plt, date_times[P:Q], output[P:Q, :cambermet_depth]; label=:cambermet_depth);

# # plt = plot();
# # plot!(plt, date_times[P:Q], output[P:Q, :sotonmet_wind_speed]; label=:sotonmet_wind_speed);
# # plot!(plt, date_times[P:Q], output[P:Q, :bramblemet_wind_speed]; label=:bramblemet_wind_speed);
# # plot!(plt, date_times[P:Q], output[P:Q, :chimet_wind_speed]; label=:chimet_wind_speed);
# # plot!(plt, date_times[P:Q], output[P:Q, :cambermet_wind_speed]; label=:cambermet_wind_speed);

# plt = plot();
# plot!(plt, date_times[P:Q], output[P:Q, :sotonmet_air_temp]; label=:sotonmet_air_temp);
# plot!(plt, date_times[P:Q], output[P:Q, :bramblemet_air_temp]; label=:bramblemet_air_temp);
# plot!(plt, date_times[P:Q], output[P:Q, :chimet_air_temp]; label=:chimet_air_temp);
# plot!(plt, date_times[P:Q], output[P:Q, :cambermet_air_temp]; label=:cambermet_air_temp);

# display(plt);
