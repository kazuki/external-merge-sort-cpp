#include <atomic>
#include <chrono>
#include <iostream>
#include <random>

#include "external_merge_sort.hpp"

using namespace MultithreadedExternalMergeSort;
using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;
using seconds = std::chrono::duration<double>;

template <typename T>
void generate(T* output, size_t start, size_t end);

template <>
void generate<std::tuple<uint64_t, uint64_t>>(
    std::tuple<uint64_t, uint64_t>* output, size_t start, size_t end) {
  std::mt19937 gen(start);
  std::uniform_int_distribution<uint64_t> dist;

  for (auto i = start; i < end; ++i) {
    std::get<0>(output[i - start]) = dist(gen);
    std::get<1>(output[i - start]) = dist(gen);
  }
}

template <>
void generate<std::tuple<uint32_t, uint32_t>>(
    std::tuple<uint32_t, uint32_t>* output, size_t start, size_t end) {
  std::mt19937 gen(start);
  std::uniform_int_distribution<uint32_t> dist;

  for (auto i = start; i < end; ++i) {
    std::get<0>(output[i - start]) = dist(gen);
    std::get<1>(output[i - start]) = dist(gen);
  }
}

template <>
void generate<std::tuple<uint32_t, uint32_t, uint32_t>>(
    std::tuple<uint32_t, uint32_t, uint32_t>* output, size_t start,
    size_t end) {
  std::mt19937 gen(start);
  std::uniform_int_distribution<uint32_t> dist;

  for (auto i = start; i < end; ++i) {
    std::get<0>(output[i - start]) = dist(gen);
    std::get<1>(output[i - start]) = dist(gen);
    std::get<2>(output[i - start]) = dist(gen);
  }
}

template <>
void generate<uint64_t>(uint64_t* output, size_t start, size_t end) {
  std::mt19937 gen(start);
  std::uniform_int_distribution<uint64_t> dist;

  for (auto i = start; i < end; ++i) {
    output[i - start] = dist(gen);
  }
}

template <typename T>
std::tuple<T, T> operator^(const std::tuple<T, T>& lhs,
                           const std::tuple<T, T>& rhs) {
  return std::make_tuple(std::get<0>(lhs) ^ std::get<0>(rhs),
                         std::get<1>(lhs) ^ std::get<1>(rhs));
}

template <typename T>
std::tuple<T, T, T> operator^(const std::tuple<T, T, T>& lhs,
                              const std::tuple<T, T, T>& rhs) {
  return std::make_tuple(std::get<0>(lhs) ^ std::get<0>(rhs),
                         std::get<1>(lhs) ^ std::get<1>(rhs),
                         std::get<2>(lhs) ^ std::get<2>(rhs));
}

// using KeyType = uint64_t;
// using KeyType = std::tuple<uint64_t, uint64_t>;
// using KeyType = std::tuple<uint32_t, uint32_t>;
using KeyType = std::tuple<uint32_t, uint32_t, uint32_t>;

int main() {
  const size_t generate_keys = static_cast<size_t>(16) << 27;
  const size_t segment_size = 1024 * 1024 * 192 / sizeof(KeyType);

  ExternalMergeSorter<KeyType> sorter;
  const char* tmp_path = "external_sort_bench.tmp";
  int fd = ::open(tmp_path, O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
  if (fd < 0) throw std::runtime_error("open failed");
  ::unlink(tmp_path);

  KeyType expected{};
  {
    auto st = high_resolution_clock::now();
    std::mutex m;
    std::atomic_uint64_t off_bytes = 0;
    std::atomic_uint64_t generated_key_count = 0;
    std::vector<std::thread> threads;
    for (unsigned i = 0; i < std::thread::hardware_concurrency(); ++i) {
      threads.emplace_back([&]() {
        std::vector<KeyType> buf(segment_size);
        while (1) {
          KeyType tmp{};
          auto off = generated_key_count.fetch_add(buf.size());
          if (off >= generate_keys) break;
          auto size = std::min(buf.size(), generate_keys - off);
          generate(buf.data(), off, off + size);
          if (size < buf.size()) buf.resize(size);
          std::sort(buf.begin(), buf.end());
          for (auto& x : buf) tmp = tmp ^ x;
          auto write_bytes = size * sizeof(KeyType);
          auto write_pos = off_bytes.fetch_add(size * sizeof(KeyType));
          size_t wrote_bytes = 0;
          while (wrote_bytes < write_bytes) {
            auto ret = ::pwrite(
                fd, reinterpret_cast<const char*>(buf.data()) + wrote_bytes,
                write_bytes - wrote_bytes, write_pos + wrote_bytes);
            if (ret <= 0) throw std::runtime_error("pwrite failed");
            wrote_bytes += ret;
          }
          {
            std::lock_guard<std::mutex> lg(m);
            sorter.AddSortedSegment(fd, write_pos, size);
            expected = expected ^ tmp;
          }
        }
      });
    }
    for (auto& t : threads) t.join();
    auto elapsed =
        duration_cast<seconds>(high_resolution_clock::now() - st).count();
    std::cout << "Generate: " << (generate_keys / elapsed / 1000000.0)
              << " M keys/s (" << elapsed << "s)" << std::endl;
  }

  {
    auto st = high_resolution_clock::now();
    size_t count = 0;
    KeyType actual{};
    KeyType prev;
    bool failed = false;
    sorter.Execute([&](const KeyType& x) {
      if (count > 0 && prev > x) failed = true;
      count++;
      prev = x;
      actual = actual ^ x;
    });
    auto elapsed =
        duration_cast<seconds>(high_resolution_clock::now() - st).count();
    std::cout << "Sort: " << (generate_keys / elapsed / 1000000) << " M keys/s"
              << std::endl;
    if (count != generate_keys)
      std::cout << "VALIDATION FAILED (count mismatch " << count
                << " != " << generate_keys << ")" << std::endl;
    if (failed) std::cout << "VALIDATION FAILED (unsorted)" << std::endl;
    if (expected != actual)
      std::cout << "VALIDATION FAILED (wrong XOR value)" << std::endl;
  }
}
