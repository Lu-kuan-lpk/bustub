//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  auto Victim(frame_id_t *frame_id) -> bool override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  auto Size() -> size_t override;

 private:
  // TODO(student): implement me!
  class DqueueNode {
   public:
    DqueueNode *pre_ = {nullptr};
    DqueueNode *next_ = {nullptr};
    frame_id_t cur_frame_;
    explicit DqueueNode(frame_id_t frame_id) { cur_frame_ = frame_id; }
  };
  std::unordered_map<frame_id_t, DqueueNode *> node_map_;
  DqueueNode *dummy_;
  DqueueNode *tail_;
  void AddToHead(DqueueNode *node);
  void RemoveNode(DqueueNode *node);
  int size_;
  int capacity_;
  auto GetTail() -> frame_id_t;
  std::mutex latch_;
};

}  // namespace bustub
