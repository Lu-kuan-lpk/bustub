//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.push_back(std::make_shared<Bucket>(bucket_size, global_depth_));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOfDepth(const K &key, const int &depth) -> size_t {
  int mask = (1 << depth) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  return dir_[IndexOf(key)]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  std::lock_guard<std::mutex> guard(latch_);
  return dir_[IndexOf(key)]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // UNREACHABLE("not implemented");
  std::lock_guard<std::mutex> guard(latch_);
  while (true) {
    bool success = dir_[IndexOf(key)]->Insert(key, value);
    if (!success) {
      RedistributeBucket(dir_[IndexOf(key)], IndexOf(key));
    } else {
      break;
    }
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  bool flag = false;
  for (const auto &pair : list_) {
    if (pair.first == key) {
      value = pair.second;
      flag = true;
    }
  }
  return flag;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  bool flag = false;
  for (auto it = list_.begin(); it != list_.end();) {
    if ((*it).first == key) {
      list_.erase(it++);
      flag = true;
    } else {
      it++;
    }
  }
  return flag;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // UNREACHABLE("not implemented");
  bool flag = false;
  for (auto it = list_.begin(); it != list_.end();) {
    if ((*it).first == key) {
      (*it).second = value;
      flag = true;
    } else {
      it++;
    }
  }
  if (!flag) {
    if (list_.size() != size_) {
      list_.push_back(std::pair<K, V>(key, value));
      return true;
    }
    return false;
  }
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket, size_t index) -> void {
  LOG_INFO("bucket with the index of %d split", (int)index);
  // distribute the bucket into two bucket
  if (bucket->GetDepth() == global_depth_) {
    // add new bucket
    for (int i = 0; i < 1 << global_depth_; i++) {
      // copy the original bucket
      dir_.push_back(dir_[i]);
    }
    global_depth_++;
  }

  // add the depth
  bucket->IncrementDepth();

  auto list = bucket->GetItems();
  int split_idx = static_cast<int>(index) >= (1 << (global_depth_ - 1)) ? index - (1 << (global_depth_ - 1))
                                                                        : index + (1 << (global_depth_ - 1));
  dir_[split_idx] = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());
  for (auto it = list.begin(); it != list.end(); it++) {
    if (IndexOfDepth((*it).first, bucket->GetDepth()) != index) {
      dir_[index]->Remove((*it).first);
      dir_[IndexOfDepth((*it).first, bucket->GetDepth())]->Insert((*it).first, (*it).second);
    }
  }
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
