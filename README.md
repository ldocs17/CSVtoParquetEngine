# High-Performance Parallel CSV-to-Parquet ETL Engine

A production-grade, cross-platform C++ data ingestion engine capable of parsing unstructured text data and serializing it into compressed, analytical Parquet formats. By pairing zero-copy OS memory-mapping with strict data isolation, this architecture completely bypasses common performance bottlenecks like thread contention, heap allocation loops, and runtime buffer resizing.

## 🚀 Performance Benchmarks
Tested on a multi-million row financial dataset against a standard single-threaded implementation utilizing traditional C++ I/O streams (`std::ifstream`, `std::getline`, and `std::stringstream`).

| Pipeline Phase | Naive Baseline Stream | Memory-Mapped Parallel Engine | Speedup |
| :--- | :--- | :--- | :--- |
| **Parsing Latency** | 38,215 ms | 1,599 ms | **23.9x Faster** |
| **Parquet Serialization** | 2,500 ms | 2,246 ms | N/A (Shared Driver) |
| **Total End-to-End Pipeline** | 40,715 ms | 3,845 ms | **10.6x Faster** |

## 🏗️ Architectural Core

### 1. Zero-Copy Ingestion
Instead of loading chunks into runtime application buffers, the engine maps files directly into virtual memory via `arrow::io::MemoryMappedFile`. String tokens are manipulated purely as `std::string_view` windows pointing to raw data, ensuring a memory footprint of exactly zero additional string heap allocations during tokenization.

### 2. Lock-Free Boundary Alignment
To process files concurrently without data overlap or dropped records, the file is divided into arbitrary byte chunks. Individual worker threads execute a forward scan to advance their boundary to the nearest trailing newline (`\n`), allowing completely isolated, synchronized data processing without system locks.

### 3. Dynamic Memory Reservation
To avoid memory reallocation cascades, the system uses a dynamic sampling algorithm over the initial 50 rows of the data stream to accurately estimate string and numeric density, pre-allocating the underlying Apache Arrow Array Buffers via `.Reserve()` and `.ReserveData()`.