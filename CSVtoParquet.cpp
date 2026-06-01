#include <iostream>
#include <fstream>
#include <string>
#include <string_view>
#include <filesystem>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include "arrow/util/type_fwd.h"
#include <chrono>
#include <charconv>
#include <vector>
#include <future>

/* This version of the parser uses memory-mapped files to read the CSV data directly from disk without loading it all into memory at once.
   Instead of using the windows.h API directly, this implementation uses Arrow's memory-mapped file utilities which provide a more
   cross-platform interface for working with memory-mapped files.
   It divides the file into chunks and processes each chunk in parallel using multiple threads, which can speed up the parsing for large files.
   The data is parsed into Arrow RecordBatches which are then combined into a single Table for output.
   This approach minimizes memory usage and maximizes performance by leveraging the operating system's virtual memory management and efficient I/O operations.
*/

using Clock = std::chrono::high_resolution_clock;
using MS = std::chrono::milliseconds;

// This function can be used for streaming scenarios where chunks of data are converted into RecordBatches.
arrow::Result<std::shared_ptr<arrow::RecordBatch>> dataToRecordBatch(
    const char* data_ptr,
    const char* data_end,
    const std::shared_ptr<arrow::Schema>& schema,
    const int expected_rows)
{
    // Preparing the builders for each column
    arrow::Int32Builder idBuilder;
    arrow::StringBuilder nameBuilder;
    arrow::Int32Builder ageBuilder;
    arrow::StringBuilder acctTypeBuilder;
    arrow::DoubleBuilder paymentBuilder;

    // Pre-allocates memory for the builders to improve performance.
    int expect_number_acct_bytes = expected_rows * 8;
    int expect_number_name_bytes = expected_rows * 20; // Assumes an average of 20 bytes per name string.

    ARROW_RETURN_NOT_OK(idBuilder.Reserve(expected_rows));
    ARROW_RETURN_NOT_OK(ageBuilder.Reserve(expected_rows));
    ARROW_RETURN_NOT_OK(paymentBuilder.Reserve(expected_rows));
    ARROW_RETURN_NOT_OK(acctTypeBuilder.Reserve(expected_rows));
    ARROW_RETURN_NOT_OK(nameBuilder.Reserve(expected_rows));

    // Reserves space for the actual string data in the builders.
    // Over-estimating is intentional to avoid reallocation costs.
    ARROW_RETURN_NOT_OK(acctTypeBuilder.ReserveData(expect_number_acct_bytes));
    ARROW_RETURN_NOT_OK(nameBuilder.ReserveData(expect_number_name_bytes));

    while (data_ptr < data_end) {
        if (*data_ptr == ('\n') || *data_ptr == ('\r')) { // Skip empty lines
            ++data_ptr;
            continue;
        }

        // Parse each line manually
        int id;
        auto id_end = std::from_chars(data_ptr, data_end, id);
        if (id_end.ec != std::errc()) return arrow::Status::Invalid("Failed to parse id");
        data_ptr = id_end.ptr + 1;
        ARROW_RETURN_NOT_OK(idBuilder.Append(id));

        // Scans for the comma delimiter and appends the substring directly from the memory.
        const char* name_start = data_ptr;
        while (data_ptr < data_end && *data_ptr != ',') ++data_ptr;
        if (data_ptr == data_end) return arrow::Status::Invalid("Unexpected end of line while parsing name");
        ARROW_RETURN_NOT_OK(nameBuilder.Append(std::string_view(name_start, data_ptr - name_start)));
        ++data_ptr;

        int age;
        auto age_end = std::from_chars(data_ptr, data_end, age);
        if (age_end.ec != std::errc()) return arrow::Status::Invalid("Failed to parse age");
        data_ptr = age_end.ptr + 1;
        ARROW_RETURN_NOT_OK(ageBuilder.Append(age));

        const char* acctType_start = data_ptr;
        while (data_ptr < data_end && *data_ptr != ',') ++data_ptr;
        if (data_ptr == data_end) return arrow::Status::Invalid("Unexpected end of line while parsing acctType");
        ARROW_RETURN_NOT_OK(acctTypeBuilder.Append(std::string_view(acctType_start, data_ptr - acctType_start)));
        ++data_ptr;

        double payment;
        auto payment_end = std::from_chars(data_ptr, data_end, payment);
        if (payment_end.ec != std::errc()) return arrow::Status::Invalid("Failed to parse payment");
        data_ptr = payment_end.ptr;
        ARROW_RETURN_NOT_OK(paymentBuilder.Append(payment));

        while (data_ptr < data_end && *data_ptr != '\n' && *data_ptr != '\r') ++data_ptr; // moves to end of line
        if (data_ptr < data_end) ++data_ptr; // skips newline
    }

    std::shared_ptr<arrow::Array> id_array;
    std::shared_ptr<arrow::Array> name_array;
    std::shared_ptr<arrow::Array> age_array;
    std::shared_ptr<arrow::Array> acctType_array;
    std::shared_ptr<arrow::Array> payment_array;

    ARROW_RETURN_NOT_OK(idBuilder.Finish(&id_array));
    ARROW_RETURN_NOT_OK(nameBuilder.Finish(&name_array));
    ARROW_RETURN_NOT_OK(ageBuilder.Finish(&age_array));
    ARROW_RETURN_NOT_OK(acctTypeBuilder.Finish(&acctType_array));
    ARROW_RETURN_NOT_OK(paymentBuilder.Finish(&payment_array));

    auto recordBatch = arrow::RecordBatch::Make(
        schema,
        id_array->length(),
        { id_array, name_array, age_array, acctType_array, payment_array });
    return recordBatch;
}

arrow::Result<std::shared_ptr<arrow::Table>> parseCSVtoTable(const std::string& filename, int number_of_threads)
{
    if (number_of_threads <= 0) {
        number_of_threads = 1; // Default to single thread if invalid number provided
    }
    if (number_of_threads > std::thread::hardware_concurrency()) {
        number_of_threads = std::thread::hardware_concurrency(); // Cap at available threads to prevent excessive overhead
    }

    // Gather file size
    std::error_code ec;
    std::size_t file_size = std::filesystem::file_size(filename, ec);
    if (ec) {
        return arrow::Status::IOError("Failed to get file size.", ec.message());
    }
    if (file_size == 0) {
        return arrow::Status::IOError("File is empty");
    }

    std::shared_ptr<arrow::io::MemoryMappedFile> mm_file;
    ARROW_ASSIGN_OR_RAISE(mm_file, arrow::io::MemoryMappedFile::Open(filename, arrow::io::FileMode::READ));
    std::size_t chunk_size = file_size / number_of_threads;

    // Using Arrow to create a memory-mapped file which abstracts away platform specific details
    std::shared_ptr<arrow::Buffer> buffer;
    ARROW_ASSIGN_OR_RAISE(buffer, mm_file->Read(file_size));
    const char* data_ptr = reinterpret_cast<const char*>(buffer->data());

    auto schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("name", arrow::utf8()),
        arrow::field("age", arrow::int32()),
        arrow::field("acctType", arrow::utf8()),
        arrow::field("payment", arrow::float64())
    });

    // Calculates a row count estimate by sampling the first 50 lines.
    std::size_t line_Count = 0;
    std::size_t total_lines_estimated = 50;

    const char* line_counter = data_ptr;
    const char* data_end = data_ptr + file_size;

    while (line_Count < total_lines_estimated && line_counter < data_end) {
        if (*line_counter == ('\n') || *line_counter == ('\r')) { // skips empty lines
            ++line_counter;
            continue;
        }
        while (line_counter < data_end && *line_counter != '\n' && *line_counter != '\r') ++line_counter; // moves to end of line
        if (line_counter < data_end) ++line_counter; // skips newline
        ++line_Count;
    }
    if (line_Count < total_lines_estimated) {
        total_lines_estimated = line_Count; // this will adjust estimate down if needed.
    }
    std::size_t bytes_sampled = line_counter - data_ptr;
    int estimated_rows = static_cast<int>((static_cast<double>(file_size) / bytes_sampled * total_lines_estimated) * 1.15); //15% buffer
    int estimated_rows_per_thread = estimated_rows / number_of_threads;

    std::vector<std::future<arrow::Result<std::shared_ptr<arrow::RecordBatch>>>> async_tasks;

    for (int i = 0; i < number_of_threads; ++i) {
        std::size_t chunk_start = i * chunk_size;
        std::size_t chunk_end = (i == number_of_threads - 1) ? file_size : (i + 1) * chunk_size;

        if (i == 0) {
            // thread 0 begins at byte 0 and includes the header row. Space for parsing header if needed.
        } else {
            // all other threads skip their initial partial row.
            while (chunk_start < file_size && data_ptr[chunk_start] != '\n') {
                ++chunk_start;
            }
            if (chunk_start < file_size) {
                ++chunk_start; // moves past '\n' so this thread starts exactly at character 0 of a fresh row
            }
        }

        // All threads advance their end boundary to include the complete row in progress.
        if (i != number_of_threads - 1) {
            while (chunk_end < file_size && data_ptr[chunk_end] != '\n') {
                ++chunk_end;
            }
            if (chunk_end < file_size) {
                ++chunk_end; // Move past '\n' so the thread captures the ending delimiter cleanly
            }
        }

        // chunk boundaries are now aligned to row boundaries no bytes are dropped or double-counted.
        async_tasks.emplace_back(std::async(
            std::launch::async,
            dataToRecordBatch,
            data_ptr + chunk_start,
            data_ptr + chunk_end,
            schema,
            estimated_rows_per_thread));
    }

    std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;
    for (auto& task : async_tasks) {
        auto result = task.get();
        if (!result.ok()) {
            return result.status(); // Return error if any thread failed
        }
        record_batches.push_back(result.ValueOrDie());
    }

    auto recordTable = arrow::Table::FromRecordBatches(schema, record_batches);
    return recordTable;
}

arrow::Status tableToParquet(const arrow::Table& table)
{
    using parquet::WriterProperties;
    using parquet::ArrowWriterProperties;
    // Sets compression to LZ4.
    std::shared_ptr<WriterProperties> props = WriterProperties::Builder().compression(arrow::Compression::LZ4)->build();

    // Stores the Arrow schema in the Parquet metadata for future reads.
    std::shared_ptr<ArrowWriterProperties> arrowProps = ArrowWriterProperties::Builder().store_schema()->build();

    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test.parquet"));

    ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(table, arrow::default_memory_pool(), outfile, 1000000, props, arrowProps));

    return arrow::Status::OK();
}

int main()
{
    const int number_of_threads = 8;
    std::string filename = "dummy_financial_data.csv";

    // Parser
    auto t0 = Clock::now();
    arrow::Result<std::shared_ptr<arrow::Table>> result = parseCSVtoTable(filename, number_of_threads);
    auto t1 = Clock::now();
    auto duration = std::chrono::duration_cast<MS>(t1 - t0).count();
    std::cout << "Time taken to parse CSV: " << duration << " ms" << std::endl;

    std::shared_ptr<arrow::Table> table;
    if (result.ok()) {
        table = result.ValueOrDie();
    } else {
        std::cerr << "Error creating Arrow Table: " << result.status().ToString() << std::endl;
        return 1;
    }

    // Write to Parquet
    auto t2 = Clock::now();
    arrow::Status status = tableToParquet(*table);
    auto t3 = Clock::now();
    auto duration2 = std::chrono::duration_cast<MS>(t3 - t2).count();
    std::cout << "Time taken to write Parquet: " << duration2 << " ms" << std::endl;

    std::cout << "Total time taken: " << (duration + duration2) << " ms" << std::endl;
    return 0;
}
