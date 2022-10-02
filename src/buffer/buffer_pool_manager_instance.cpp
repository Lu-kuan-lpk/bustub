//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include <cstddef>
// #include <mutex>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::lock_guard<std::recursive_mutex> guard(latch_);

  bool allocate = false;
  // check whether it could be allocate
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPinCount() == 0) {
      allocate = true;
    }
  }
  if (!allocate) {
    return nullptr;
  }

  // we firstly find in freelist, then find the replacer
  frame_id_t frame_id;
  if (free_list_.empty()) {
    bool success = replacer_->Evict(&frame_id);
    if (!success) {
      return nullptr;
    }
  } else {
    frame_id = free_list_.back();
    free_list_.pop_back();
  }

  // we should set the data of the original page to the disk using the disk_manager
  Page &pre_page = pages_[frame_id];
  if (pre_page.IsDirty()) {
    disk_manager_->WritePage(pre_page.GetPageId(), pre_page.GetData());
    pre_page.is_dirty_ = false;
  }
  page_table_->Remove(pre_page.GetPageId());

  // we allocate the new page
  *page_id = AllocatePage();
  Page *page = &pages_[frame_id];
  page->page_id_ = *page_id;
  page->pin_count_++;

  page_table_->Insert(*page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::lock_guard<std::recursive_mutex> guard(latch_);

  frame_id_t frame_id;
  // find isn't in the buffer pool && all pages is pinned
  if (!page_table_->Find(page_id, frame_id)) {
    bool allpin = true;
    for (size_t i = 0; i < pool_size_; i++) {
      if (pages_[i].GetPinCount() == 0) {
        allpin = false;
        break;
      }
    }
    if (!allpin) {
      // we should fetch it from the disk

      // we firstly find in freelist, then find the replacer
      if (free_list_.empty()) {
        bool success = replacer_->Evict(&frame_id);
        if (!success) {
          return nullptr;
        }
      } else {
        frame_id = free_list_.back();
        free_list_.pop_back();
      }

      // we should set the data of the original page to the disk using the disk_manager
      Page &pre_page = pages_[frame_id];
      if (pre_page.IsDirty()) {
        disk_manager_->WritePage(pre_page.GetPageId(), pre_page.GetData());
        pre_page.is_dirty_ = false;
      }
      page_table_->Remove(pre_page.GetPageId());

      // we allocate the new page
      Page *page = &pages_[frame_id];
      disk_manager_->ReadPage(page_id, page->data_);
      page->page_id_ = page_id;
      page->pin_count_++;

      page_table_->Insert(page_id, frame_id);
      replacer_->RecordAccess(frame_id);
      replacer_->SetEvictable(frame_id, false);
    } else {
      return nullptr;
    }
  }
  // the page is in the buffer pool
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::recursive_mutex> guard(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id) || pages_[frame_id].GetPinCount() == 0) {
    return false;
  }

  if (is_dirty) {
    pages_[frame_id].is_dirty_ = true;
  }

  if (pages_[frame_id].pin_count_-- == 1) {
    // set evictable
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::recursive_mutex> guard(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  // flush the page data into the memory and reset the is_dirty
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::lock_guard<std::recursive_mutex> guard(latch_);

  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPageId() != INVALID_PAGE_ID) {
      disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
      pages_[i].is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::recursive_mutex> guard(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }

  // the page is pinned and could not be evicted
  if (pages_[frame_id].GetPinCount() > 0) {
    return false;
  }

  // delete the page : page_table, pages, free_list
  if (pages_[frame_id].IsDirty()) {
    FlushPgImp(page_id);
  }
  page_table_->Remove(page_id);
  free_list_.push_back(frame_id);
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].ResetMemory();

  return false;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
