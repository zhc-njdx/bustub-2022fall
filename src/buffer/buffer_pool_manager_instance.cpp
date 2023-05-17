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

#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  LOG_INFO("The buffer_pool_manager_instance has created, the pool_size is %zu.\n", pool_size);
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

/**
 * TODO(P1): Add implementation
 *
 * @brief Create a new page in the buffer pool. Set page_id to the new page's id, or nullptr if all frames
 * are currently in use and not evictable (in another word, pinned).
 *
 * You should pick the replacement frame from either the free list or the replacer (always find from the free list
 * first), and then call the AllocatePage() method to get a new page id. If the replacement frame has a dirty page,
 * you should write it back to the disk first. You also need to reset the memory and metadata for the new page.
 *
 * Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
 * so that the replacer wouldn't evict the frame before the buffer pool manager "Unpin"s it.
 * Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
 *
 * @param[out] page_id id of created page
 * @return nullptr if no new pages could be created, otherwise pointer to new page
 */
auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  LOG_INFO("Want to New Page, ");
  frame_id_t frame_id;
  if (!free_list_.empty()) {  // has the free frame id
    frame_id = free_list_.front();
    free_list_.pop_front();

    *page_id = AllocatePage();
    pages_[frame_id].page_id_ = *page_id;
    page_table_->Insert(*page_id, frame_id);  // create the new mapping
    pages_[frame_id].pin_count_ = 1;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    LOG_INFO("using free frame_id %d ", frame_id);
    LOG_INFO("to new page %d\n", *page_id);
    return &pages_[frame_id];
  }
  if (replacer_->Evict(&frame_id)) {  // no free frame but can evict a frame from replacer
    // write the dirty page
    if (pages_[frame_id].IsDirty()) {
      LOG_INFO("write back to the disk");
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    }
    LOG_INFO("evicting the page %d in frame %d ", pages_[frame_id].GetPageId(), frame_id);
    page_table_->Remove(pages_[frame_id].GetPageId());  // remove the previous mapping
    replacer_->Remove(frame_id);                        // remove the access history for this page

    ResetPage(frame_id);

    *page_id = AllocatePage();
    LOG_INFO("to new page %d\n", *page_id);
    pages_[frame_id].page_id_ = *page_id;
    page_table_->Insert(*page_id, frame_id);  // overwrite the mapping
    pages_[frame_id].pin_count_ = 1;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    return &pages_[frame_id];
  }
  LOG_INFO("fail\n");
  return nullptr;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Fetch the requested page from the buffer pool. Return nullptr if page_id needs to be fetched from the disk
 * but all frames are currently in use and not evictable (in another word, pinned).
 *
 * First search for page_id in the buffer pool. If not found, pick a replacement frame from either the free list or
 * the replacer (always find from the free list first), read the page from disk by calling disk_manager_->ReadPage(),
 * and replace the old page in the frame. Similar to NewPgImp(), if the old page is dirty, you need to write it back
 * to disk and update the metadata of the new page
 *
 * In addition, remember to disable eviction and record the access history of the frame like you did for NewPgImp().
 *
 * @param page_id id of page to be fetched
 * @return nullptr if page_id cannot be fetched, otherwise pointer to the requested page
 */
auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  LOG_INFO("Want to fetch the page %d ", page_id);
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {  // find the page and return the pointer
    replacer_->RecordAccess(frame_id);
    pages_[frame_id].pin_count_++;
    if (pages_[frame_id].GetPinCount() > 0) {  // pinned
      LOG_INFO("(set non-evictable)");
      replacer_->SetEvictable(frame_id, false);
    }
    LOG_INFO("find\n");
    return &pages_[frame_id];
  }
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();

    pages_[frame_id].page_id_ = page_id;
    page_table_->Insert(page_id, frame_id);  // create the new mapping
    pages_[frame_id].pin_count_ = 1;         // initialize to 1
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
    LOG_INFO("using free frame %d\n", frame_id);
    return &pages_[frame_id];
  }
  if (replacer_->Evict(&frame_id)) {
    if (pages_[frame_id].IsDirty()) {
      LOG_INFO("write back to the disk");
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    }
    LOG_INFO("evicting the page %d in frame %d\n", pages_[frame_id].GetPageId(), frame_id);
    page_table_->Remove(pages_[frame_id].GetPageId());  // remove the previous mapping
    replacer_->Remove(frame_id);

    ResetPage(frame_id);

    pages_[frame_id].page_id_ = page_id;
    page_table_->Insert(page_id, frame_id);
    pages_[frame_id].pin_count_ = 1;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());

    return &pages_[frame_id];
  }
  LOG_INFO("fail\n");
  return nullptr;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Unpin the target page from the buffer pool. If page_id is not in the buffer pool or its pin count is already
 * 0, return false.
 *
 * Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
 * Also, set the dirty flag on the page to indicate if the page was modified.
 *
 * @param page_id id of page to be unpinned
 * @param is_dirty true if the page should be marked as dirty, false otherwise
 * @return false if the page is not in the page table or its pin count is <= 0 before this call, true otherwise
 */
auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  LOG_INFO("Want to unpin the page %d ", page_id);
  if (is_dirty) {
    LOG_INFO("and set page dirty ");
  } else {
    LOG_INFO("and set page undirty");
  }
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    LOG_INFO("not found\n");
    return false;
  }
  // ATTENTION: do the assignment only when the is_dirty = true
  // it will be easy to make mistakes
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = is_dirty;  // set the dirty bit
  }
  if (pages_[frame_id].GetPinCount() == 0) {
    // not found or pin_count == 0
    LOG_INFO("already unpinned\n");
    return false;
  }
  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].GetPinCount() == 0) {
    // pin_count == 0 => evictable
    LOG_INFO("(set evictable)");
    replacer_->SetEvictable(frame_id, true);
  }
  LOG_INFO("unpin\n");
  return true;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Flush the target page to disk.
 *
 * Use the DiskManager::WritePage() method to flush a page to disk, REGARDLESS of the dirty flag.
 * Unset the dirty flag of the page after flushing.
 *
 * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
 * @return false if the page could not be found in the page table, true otherwise
 */
auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  LOG_INFO("Want to flush page %d, ", page_id);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    LOG_INFO("not found\n");
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;
  LOG_INFO("success\n");
  return true;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Flush all the pages in the buffer pool to disk.
 */
void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPageId() != INVALID_PAGE_ID) {
      FlushPgImp(pages_[i].GetPageId());
    }
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Delete a page from the buffer pool. If page_id is not in the buffer pool, do nothing and return true. If the
 * page is pinned and cannot be deleted, return false immediately.
 *
 * After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
 * back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
 * imitate freeing the page on the disk.
 *
 * @param page_id id of page to be deleted
 * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
 */
auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  LOG_INFO("Want to delete page %d, ", page_id);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {  // not found
    LOG_INFO("not found\n");
    return true;
  }
  if (pages_[frame_id].GetPinCount() > 0) {  // is pinned
    LOG_INFO("can't delete for pinned\n");
    return false;
  }
  if (pages_[frame_id].IsDirty()) {
    LOG_INFO("write back to the disk");
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  }

  ResetPage(frame_id);
  replacer_->Remove(frame_id);
  page_table_->Remove(page_id);

  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  LOG_INFO("deleted\n");
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
