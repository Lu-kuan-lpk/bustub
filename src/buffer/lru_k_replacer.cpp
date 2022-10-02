//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cstddef>
#include <queue>
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  // lru_heap_ = std::priority_queue<HeapNode>();
  node_map_.clear();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();
  *frame_id = DeleteMaxInMap();
  latch_.unlock();
  if(*frame_id!=0) {
    LOG_INFO("evict the frame: %d",*frame_id);
    return true;
  }      
  return false;

}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  latch_.lock();
  AddToMap(frame_id);
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  // do not delete the element, just let it could not be seen
  if(node_map_.find(frame_id)!=node_map_.end()){
    if(node_map_[frame_id].evict_ && !set_evictable) {
        curr_size_--;
    }
    else if(!node_map_[frame_id].evict_ && set_evictable) {
        curr_size_++;
    }
    node_map_[frame_id].evict_ = set_evictable;
  } else {
    throw bustub::Exception("invalid frame_id");
  }
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  if(node_map_[frame_id].evict_) {
    node_map_.erase(frame_id);
  } else {
    throw bustub::Exception("remove a unevicted frame");
  }
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

void LRUKReplacer::AddToMap(frame_id_t frame_id) {
    LOG_INFO("add the frame_id: %d, time_stamp: %d",frame_id,static_cast<frame_id_t>(current_timestamp_));
  // currently donot contain the frame
  if(static_cast<size_t>(frame_id)>replacer_size_){
    throw bustub::Exception("frame_id is larger than the capacity of the replacer");
  }
  if (node_map_.find(frame_id) == node_map_.end()) {
    // already full
    if (curr_size_ == replacer_size_) {
      // do nothing
    } else {
      HeapNode cur_node = HeapNode(static_cast<size_t>(k_));
      node_map_[frame_id] = cur_node;
      // lru_heap_.push(cur_node);
      curr_size_++;
    }
  } 
  node_map_[frame_id].AddTimeStamp(current_timestamp_);
  current_timestamp_++;
}

auto LRUKReplacer::DeleteMaxInMap() -> frame_id_t  {
  frame_id_t frame = 0;
  HeapNode maxnode = HeapNode();
  for (const auto& kv : node_map_) {
    LOG_INFO("%d,%d",kv.first,kv.second.evict_);
    if (kv.second.evict_) {
        LOG_INFO("%d",static_cast<int32_t>(kv.second.GetDistance()));
      if (maxnode.timestamps_.empty() || maxnode<kv.second) {
        frame = kv.first;
        maxnode = kv.second;
      }
    }
  }
  if(frame!=0) {
    node_map_.erase(frame);
    curr_size_--;
     LOG_INFO("delete the frame_id: %d, time_stamp: %d",frame,static_cast<frame_id_t>(current_timestamp_));

  }
  LOG_INFO("here");
  
  return frame;
}

}  // namespace bustub
