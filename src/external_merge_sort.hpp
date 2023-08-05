#ifndef MULTITHREADED_EXTERNAL_MERGE_SORT_EXTERNAL_MERGE_SORT_HPP
#define MULTITHREADED_EXTERNAL_MERGE_SORT_EXTERNAL_MERGE_SORT_HPP

#include <fcntl.h>
#include <unistd.h>

#include <condition_variable>
#include <functional>
#include <iostream>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

namespace MultithreadedExternalMergeSort {

template <typename T, class Compare = std::less<T>>
class ExternalMergeSorter {
  struct SortedSegmentInfo {
    int fd;
    off_t off_bytes;
    size_t count;
  };

 public:
  ExternalMergeSorter(
      bool use_multiway_merger = false,
      std::pmr::memory_resource* resource = std::pmr::get_default_resource())
      : memory_resource_(resource),
        open_fd_cache_(),
        sorted_segments_(),
        use_multiway_(use_multiway_merger) {}

  ~ExternalMergeSorter() {
    for (const auto& [_, fd] : open_fd_cache_) {
      ::close(fd);
    }
  }

  void AddSortedSegment(int fd, off_t offset_bytes, size_t count) {
    sorted_segments_.push_back({fd, offset_bytes, count});
  }

  void AddSortedSegment(const std::string& path, off_t offset_bytes,
                        size_t count) {
    auto found_fd = open_fd_cache_.find(path);
    if (found_fd == open_fd_cache_.end()) {
      int fd = ::open(path.c_str(), O_RDONLY);
      if (fd < 0) throw std::runtime_error("open(2) failed");
      found_fd = open_fd_cache_.emplace(path, fd).first;
    }
    AddSortedSegment(found_fd->second, offset_bytes, count);
  }

  void Execute(std::function<void(const T&)> cb, unsigned max_concurrency = 0) {
    unsigned level = 0;
    unsigned buf_size = 1024 * 4;
    std::vector<std::vector<std::unique_ptr<MergeBuffer>>> buffers;
    std::vector<std::unique_ptr<UncompressedFile>> file_backends;
    std::vector<std::unique_ptr<TwoBufferMerger>> mergers_2;
    std::vector<std::unique_ptr<MultiBufferMerger>> mergers_N;
    {
      auto& file_buffers = buffers.emplace_back();
      for (const auto& info : sorted_segments_) {
        auto& inserted = file_backends.emplace_back(
            std::make_unique<UncompressedFile>(info));
        UncompressedFile* fb = inserted.get();
        file_buffers.emplace_back(std::make_unique<MergeBuffer>(
            buf_size, memory_resource_,
            [fb](MergeBuffer& mb) { fb->Fill(mb); }));
      }
    }
    std::cout << level << ": " << buffers[0].size() << std::endl;

    if (max_concurrency == 0)
      max_concurrency = std::thread::hardware_concurrency();

    while (buffers.back().size() > 1) {
      buf_size *= 2;
      level++;

      auto& current_buffers = buffers.emplace_back();
      auto& prev_buffers = buffers[buffers.size() - 2];
      unsigned N_way = 2;
      if (use_multiway_) {
        N_way = std::max(
            2u,
            std::min(4u, static_cast<unsigned>(prev_buffers.size()) /
                             std::max(1u, (max_concurrency / (2 << level)))));
      }

      size_t i = 0;
      if (N_way > 2) {
        std::vector<MergeBuffer*> v;
        v.reserve(N_way);
        for (; i < prev_buffers.size() - N_way + 1; i += N_way) {
          v.clear();
          for (size_t j = 0; j < N_way; j++)
            v.push_back(prev_buffers[i + j].get());
          auto* p =
              mergers_N.emplace_back(std::make_unique<MultiBufferMerger>(v))
                  .get();
          current_buffers.emplace_back(std::make_unique<MergeBuffer>(
              buf_size, memory_resource_,
              [p](MergeBuffer& mb) { p->Fill(mb); }));
        }
      }
      for (; i < prev_buffers.size() - 1; i += 2) {
        auto* p = mergers_2
                      .emplace_back(std::make_unique<TwoBufferMerger>(
                          prev_buffers[i].get(), prev_buffers[i + 1].get()))
                      .get();
        current_buffers.emplace_back(std::make_unique<MergeBuffer>(
            buf_size, memory_resource_, [p](MergeBuffer& mb) { p->Fill(mb); }));
      }
      if (i < prev_buffers.size())
        current_buffers.emplace_back(std::move(prev_buffers.back()));

      std::cout << level << ": " << current_buffers.size();
      if (N_way > 2) std::cout << " (" << N_way << "-way)";
      std::cout << std::endl;
    }

    auto& root = buffers.back().front();
    while (root->Next()) cb(root->Front());

    buffers.clear();
  }

 private:
  std::pmr::memory_resource* memory_resource_;
  std::unordered_map<std::string, int> open_fd_cache_;
  std::vector<SortedSegmentInfo> sorted_segments_;
  bool use_multiway_;

  struct MergeBuffer {
    std::pmr::vector<T> buf;
    std::thread thread;
    std::mutex m;
    std::condition_variable cv;
    T* rbuf;
    size_t rbuf_size;
    size_t read_idx;
    T* wbuf;
    size_t wbuf_size;
    bool eos;

    MergeBuffer(size_t size, std::pmr::memory_resource* resource,
                std::function<void(MergeBuffer&)>&& filler)
        : buf(size * 2, resource),
          m(),
          cv(),
          rbuf(buf.data()),
          rbuf_size(1),
          read_idx(0),
          wbuf(buf.data() + size),
          wbuf_size(0),
          eos(false) {
      thread = std::thread([f = std::move(filler), this]() { f(*this); });
    }

    ~MergeBuffer() { thread.join(); }

    size_t capacity() { return buf.size() / 2; }
    const T& Front() const { return rbuf[read_idx]; }

    bool Next() {
      read_idx++;
      if (read_idx == rbuf_size) {
        if (eos && wbuf_size == 0) return false;
        std::unique_lock lk(m);
        cv.notify_one();
        cv.wait(lk, [&] { return read_idx == 0; });
        if (rbuf_size == 0) return false;
      }
      return true;
    }

    void FilledBuffer(bool is_eos) {
      std::unique_lock lk(m);
      if (read_idx < rbuf_size)
        cv.wait(lk, [&] { return read_idx == rbuf_size; });
      std::swap(rbuf, wbuf);
      rbuf_size = wbuf_size;
      read_idx = 0;
      wbuf_size = 0;
      eos = is_eos;
      cv.notify_one();
    }
  };

  class UncompressedFile {
    int fd_;
    off_t off_bytes_;
    size_t count_;

   public:
    UncompressedFile(const SortedSegmentInfo& info)
        : fd_(info.fd), off_bytes_(info.off_bytes), count_(info.count) {}

    void Fill(MergeBuffer& mb) {
      while (count_ > 0) {
        size_t read_bytes = 0;
        while (count_ > 0) {
          auto pread_bytes = std::min(count_ * sizeof(T),
                                      mb.capacity() * sizeof(T) - read_bytes);
          if (pread_bytes == 0) break;
          auto ret = ::pread(fd_, reinterpret_cast<char*>(mb.wbuf) + read_bytes,
                             pread_bytes, off_bytes_);
          if (ret <= 0) throw std::runtime_error("pread failed");
          off_bytes_ += ret;
          read_bytes += ret;
          count_ -= ret / sizeof(T);
        }
        mb.wbuf_size = read_bytes / sizeof(T);
        mb.FilledBuffer(count_ == 0);
      }
    }
  };

  class TwoBufferMerger {
    MergeBuffer* lhs_;
    MergeBuffer* rhs_;

   public:
    TwoBufferMerger(MergeBuffer* lhs, MergeBuffer* rhs)
        : lhs_(lhs), rhs_(rhs) {}

    void Fill(MergeBuffer& mb) {
      if (!lhs_->Next()) lhs_ = nullptr;
      if (!rhs_->Next()) rhs_ = nullptr;
      size_t wi = 0;
      if (lhs_ && rhs_) {
        while (1) {
          if (lhs_->Front() <= rhs_->Front()) {
            mb.wbuf[wi++] = lhs_->Front();
            if (!lhs_->Next()) {
              lhs_ = nullptr;
              break;
            }
          } else {
            mb.wbuf[wi++] = rhs_->Front();
            if (!rhs_->Next()) {
              rhs_ = nullptr;
              break;
            }
          }
          if (wi == mb.capacity()) {
            mb.wbuf_size = wi;
            mb.FilledBuffer(false);
            wi = 0;
          }
        }
      }
      MergeBuffer* other = (lhs_ ? lhs_ : rhs_);
      while (1) {
        mb.wbuf[wi++] = other->Front();
        if (!other->Next()) break;
        if (wi == mb.capacity()) {
          mb.wbuf_size = wi;
          mb.FilledBuffer(false);
          wi = 0;
        }
      }
      mb.wbuf_size = wi;
      mb.FilledBuffer(true);
    }
  };

  static constexpr bool MergeBufferComp(MergeBuffer*& a, MergeBuffer*& b) {
    return b->Front() < a->Front();
  }

  class MultiBufferMerger {
    std::vector<MergeBuffer*> heap_;

   public:
    MultiBufferMerger(const std::vector<MergeBuffer*>& list) : heap_() {
      for (auto* p : list) {
        if (p->Next()) heap_.push_back(p);
      }
      std::make_heap(heap_.begin(), heap_.end(), MergeBufferComp);
    }

    void Fill(MergeBuffer& mb) {
      size_t wi = 0;
      while (heap_.size() > 1) {
        std::pop_heap(heap_.begin(), heap_.end(), MergeBufferComp);
        auto& s = heap_.back();
        mb.wbuf[wi++] = s->Front();
        if (s->Next()) {
          std::push_heap(heap_.begin(), heap_.end(), MergeBufferComp);
        } else {
          heap_.pop_back();
        }
        if (wi == mb.capacity()) {
          mb.wbuf_size = wi;
          mb.FilledBuffer(false);
          wi = 0;
        }
      }

      MergeBuffer* other = heap_.front();
      while (1) {
        mb.wbuf[wi++] = other->Front();
        if (!other->Next()) break;
        if (wi == mb.capacity()) {
          mb.wbuf_size = wi;
          mb.FilledBuffer(false);
          wi = 0;
        }
      }
      mb.wbuf_size = wi;
      mb.FilledBuffer(true);
    }
  };
};

}  // namespace MultithreadedExternalMergeSort

#endif
