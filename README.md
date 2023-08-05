# external-merge-sort-cpp

Multithreaded External Merge Sort Implementation in C++17 (Header only library)

## Usage

```c++
#include "external_merge_sort.hpp"

using DataType = std::tuple<uint32_t, uint32_t>;

int main(int, char*[]) {
  ExternalMergeSorter<DataType> sorter;

  // Write sorted segment and register to sorter
  auto seg_size = sorted_segment_bytes / sizeof(DataType);
  int fd = open("filepath", O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
  write(fd, sorted_segment_data0, sorted_segment_bytes);
  sorter.AddSortedSegment(fd, 0, seg_size);
  write(fd, sorted_segment_data1, sorted_segment_bytes);
  sorter.AddSortedSegment(fd, seg_size * sizeof(DataType), seg_size);
  write(fd, sorted_segment_data2, sorted_segment_bytes);
  sorter.AddSortedSegment(fd, seg_size * 2 * sizeof(DataType), seg_size);

  sorter.Execute([&](const DataType& x) {
    // responsed x in sorted order
  });
}
```
