//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include "common/config.h"
#include "common/logger.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
    size_ = 0;
    capacity_ = num_pages;
    node_map_.clear();
    dummy_ = new DqueueNode(0);
    tail_ = new DqueueNode(0);
    dummy_->next_ = tail_;
    tail_->pre_ = dummy_;
}

LRUReplacer::~LRUReplacer() {
    DqueueNode* node = dummy_;
    DqueueNode* next = node;
    for(int i=0;i<size_+2;i++){
        next = node->next_;
        delete node;
        node = next;
    }
}

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool { 
    bool flag = true;
    if(GetTail()==-1) { 
        flag = false;
    } else{
        *frame_id = GetTail();
        latch_.lock();
        RemoveNode(node_map_[*frame_id]);
        node_map_.erase(*frame_id);
        size_--;
        latch_.unlock();
    }
    return flag;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    // remove the frame out of the Dqueue
    latch_.lock();
    if(node_map_.find(frame_id)!=node_map_.end()) {
        RemoveNode(node_map_[frame_id]);
        node_map_.erase(frame_id);
        LOG_INFO("erase the node of %d",frame_id);
        size_--;
    }
    latch_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    // add the frame in the head of the frame and 
    LOG_INFO("into unpin");
    latch_.lock();
    if(node_map_.find(frame_id)!=node_map_.end()) {
        // ?? why do nothing (in my presume, we should put the page into the front of the queue)

        // RemoveNode(node_map_[frame_id]);
        // AddToHead(node_map_[frame_id]);
    } else {
        if(size_==capacity_){
            // get rid of the last page
            node_map_.erase(tail_->cur_frame_);        
            RemoveNode(tail_);
            size_--;
        } 
        // add the new frame_id
        auto* new_node = new DqueueNode(frame_id);
        node_map_[frame_id] = new_node;
        AddToHead(new_node);
        size_++;
    }
    latch_.unlock();
}

auto LRUReplacer::Size() -> size_t { return size_; }

void LRUReplacer::RemoveNode(DqueueNode* node) {
    LOG_INFO("start remove node: %d",node->cur_frame_);
    node->pre_->next_ = node->next_;
    node->next_->pre_ = node->pre_;
}

void LRUReplacer::AddToHead(DqueueNode* node){
    LOG_INFO("add node: %d to head",node->cur_frame_);
    node->next_ = dummy_->next_ ;
    node->pre_ = dummy_;
    dummy_->next_->pre_ = node;
    dummy_->next_ = node;
}

auto LRUReplacer::GetTail() -> frame_id_t {
    return tail_->pre_->cur_frame_;
}

}  // namespace bustub
