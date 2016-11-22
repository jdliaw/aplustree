#include "BTreeNode.h"
#include <iostream>
#include <cstring>

using namespace std;

// Constructor
BTLeafNode::BTLeafNode() {
  // initialize member variables
  numKeys = 0;
  lastIndex = 0;
  sibling = NULL;
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf) {
  return pf.read(pid, buffer);
}

/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf) {
  return pf.write(pid, buffer);
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount() {
  return numKeys;
}

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid) {
  if (getKeyCount() >= MAX_NODE_SIZE) {
    return RC_NODE_FULL; // Return an error code if the node is full.
  }

  RC rc;
  int eid;
  char* p;

  // LOCATE where to insert this key
  rc = locate(key, eid);
  int j = eid * LEAF_ENTRY_SIZE; // j = index to insert at

  // CASE: Inserting in the middle
  if (eid < numKeys) {
    // Move everything back by one entry
    for (int i = LEAF_ENTRY_SIZE * MAX_NODE_SIZE; i >= j; i--) {
      buffer[i] = buffer[i - LEAF_ENTRY_SIZE];
    }
  }

  // First append record id
  p = (char*) &rid.pid;
  for (int i = 0; i < RID_SIZE; i++) {
    // After first 4 bytes, move onto the sid
    if (i == 4) {
      p = (char*) &rid.sid;
    }
    buffer[j] = (*p);
    p++;
    j++;
  }

  // Then append the key
  p = (char*) &key; // p == first byte of key
  for (int i = 0; i < KEY_SIZE; i++) {
    buffer[j] = (*p);
    p++;
    j++;
  }

  numKeys++;

  return 0;
}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid,
                              BTLeafNode& sibling, int& siblingKey) {
  if (sibling.numKeys != 0) {
    return RC_INVALID_ATTRIBUTE; // TODO: what to return if sibling node is not empty?
  }

  // Do insert as before
  RC rc;
  int eid;
  rc = locate(key, eid);
  int j = eid * LEAF_ENTRY_SIZE;    //insert here in buffer

  //create space for insert
  if (eid < numKeys) {
    for (int i = LEAF_ENTRY_SIZE * (MAX_NODE_SIZE + 1); i >= j; i--) {
      buffer[i] = buffer[i - LEAF_ENTRY_SIZE];
    }
  }
  // insert record id
  char* p = (char*) &rid.pid;
  for (int i = 0; i < RID_SIZE; i++) {
    if (i == 4) {
      p = (char*) &rid.sid;
    }
    buffer[j] = (*p);
    p++;
    j++;
  }
  // insert key
  p = (char*) &key;
  for (int i = 0; i < KEY_SIZE; i++) {
    buffer[j] = (*p);
    p++;
    j++;
  }
  numKeys++; // End insert

  // Split the keys
  int middle = (numKeys / 2) + 1; // How many go in the left node (not sibling)
  // start at eid = middle (bc index)
  RecordId siblingRID;
  RecordId ins_rid;
  int ins_key;

  //Update first key of sibling, alternatively, memcpy?
  // TODO: Check this...
  rc = readEntry(middle, ins_key, ins_rid);
  siblingKey = ins_key;
  siblingRID = ins_rid;

  //Move the second half of this node to the first half of sibling node
  char* src = buffer + middle*LEAF_ENTRY_SIZE;
  std::memcpy(sibling.buffer, src, (numKeys-middle)*LEAF_ENTRY_SIZE);

  // Clear the second half of this node.
  char* begin = &buffer[middle * LEAF_ENTRY_SIZE];
  char* end = begin + ((numKeys - middle) * LEAF_ENTRY_SIZE);
  std::fill(begin, end, 0);

  // update numKeys
  sibling.numKeys = numKeys - middle;
  numKeys = middle;
  // update siblingID
  siblingPID = siblingRID.pid;

  return rc;
}

/**
 * If searchKey exists in the node, set eid to the index entry
 * with searchKey and return 0. If not, set eid to the index entry
 * immediately after the largest index key that is smaller than searchKey,
 * and return the error code RC_NO_SUCH_RECORD.
 * Remember that keys inside a B+tree node are always kept sorted.
 * @param searchKey[IN] the key to search for.
 * @param eid[OUT] the index entry number with searchKey or immediately
                   behind the largest key smaller than searchKey.
 * @return 0 if searchKey is found. Otherwise return an error code.
 */
RC BTLeafNode::locate(int searchKey, int& eid) {
  int rc = 0;
  int numEntries = 0;
  int curEntry = 0;
  int key;
  RecordId rid;
  while (numEntries <= numKeys) {
      rc = readEntry(curEntry, key, rid);
      if (key == searchKey) {
          eid = curEntry;
          return 0; // Found searchKey
      }
      else if (searchKey > key) { // If greater, keep searching
          curEntry++;
          numEntries++;
      }
      else {
          // If less, we want to return the current entry
          eid = curEntry;
          return RC_NO_SUCH_RECORD;
      }
  }
  // inserting at the end
  eid = numKeys;
  return rc;
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid) {
  // eid = index of each entry in the node
  // each entry = 12 bytes
  int entryIndex = eid * LEAF_ENTRY_SIZE;
  for (int i = entryIndex; i < entryIndex + LEAF_ENTRY_SIZE; i += 4) {
    if (i == entryIndex) {
      rid.pid = (int)(buffer[i+3] << 24 | buffer[i+2] << 16 | buffer[i+1] << 8 | buffer[i]);
    }
    else if (i == entryIndex+4) {
      rid.sid = (int)(buffer[i+3] << 24 | buffer[i+2] << 16 | buffer[i+1] << 8 | buffer[i]);
    }
    else {
      key = (int)(buffer[i+3] << 24 | buffer[i+2] << 16 | buffer[i+1] << 8 | buffer[i]);
    }
  }
  // TODO: what error codes can dis have?
  return 0;
}

/*
 * Return the pid of the next slibling node.
 * @return the PageId of the next sibling node
 */
PageId BTLeafNode::getNextNodePtr() {
  return siblingPID;
}

/*
 * Set the pid of the next slibling node.
 * @param pid[IN] the PageId of the next sibling node
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid) {
  siblingPID = pid;
  return 0;
}

// Constructor
BTNonLeafNode::BTNonLeafNode() {
  numKeys = 0;
  lastIndex = 0;
  siblingPID = -1;
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf) {
  return pf.read(pid, buffer);
}

/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf) {
  return pf.write(pid, buffer);
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount() {
  return numKeys;
}

/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid) {
  if (getKeyCount() >= MAX_NODE_SIZE) {
    return RC_NODE_FULL; // Return an error code if the node is full.
  }

  RC rc;
  int eid;
  char* p;

  // LOCATE where to insert this key
  rc = nonLeafLocate(key, eid);
  int j = eid * NON_LEAF_ENTRY_SIZE; // j = index to insert at

  // CASE: Inserting in the middle
  if (eid < numKeys) {
    // Move everything back by one entry
    for (int i = NON_LEAF_ENTRY_SIZE * MAX_NODE_SIZE; i >= j; i--) {
      buffer[i] = buffer[i - NON_LEAF_ENTRY_SIZE];
    }
  }

  // First append pid
  p = (char*) &pid;
  for (int i = 0; i < KEY_SIZE; i++) {
    // After first 4 bytes, move onto the sid
    buffer[j] = (*p);
    p++;
    j++;
  }

  // Then append the key
  p = (char*) &key; // p == first byte of key
  for (int i = 0; i < KEY_SIZE; i++) {
    buffer[j] = (*p);
    p++;
    j++;
  }
  numKeys++;

  return rc;
}


RC BTNonLeafNode::readNonLeafEntry(int eid, int& key, PageId& pid) {
  // eid = index of each entry in the node
  // each entry = 12 bytes
  int entryIndex = eid * NON_LEAF_ENTRY_SIZE;
  for (int i = entryIndex; i < entryIndex + NON_LEAF_ENTRY_SIZE; i += 4) {
    if (i == entryIndex) {
      pid = (int)(buffer[i+3] << 24 | buffer[i+2] << 16 | buffer[i+1] << 8 | buffer[i]);
    }
    else if (i == entryIndex+4) {
      key = (int)(buffer[i+3] << 24 | buffer[i+2] << 16 | buffer[i+1] << 8 | buffer[i]);
    }
  }
  // TODO: what error codes can dis have?
  return 0;
}

RC BTNonLeafNode::nonLeafLocate(int searchKey, int& eid) {
  RC rc;
  int numEntries = 0;
  int curEntry = 0;
  int key;
  PageId pid;

  while (numEntries <= numKeys) {
      rc = readNonLeafEntry(curEntry, key, pid);
      if (key == searchKey) {
          eid = curEntry;
          return 0; // Found searchKey
      }
      else if (searchKey > key) { // If greater, keep searching
          curEntry++;
          numEntries++;
      }
      else {
          // If less, we want to return the current entry
          eid = curEntry;
          return RC_NO_SUCH_RECORD;
      }
  }
  // inserting at the end
  eid = numKeys;
  return rc;
}


/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */

 /*
 * Insert and split:
 * Node is currently full. When inserting this node, we need to split the node
 * Node gets split down the middle, and the very middle element is discarded (moved up one level in B+Tree)
 * 1st->35th, 36th, 37th-71st node. 36th node gets returned
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey) {
  if (sibling.numKeys != 0) {
    return RC_INVALID_ATTRIBUTE; // TODO: what to return if sibling node is not empty?
  }

  // Do insert as before
  RC rc;
  int eid;
  rc = nonLeafLocate(key, eid);
  int j = eid * NON_LEAF_ENTRY_SIZE;    //insert here in buffer

  //create space for insert
  if (eid < numKeys) {
    for (int i = NON_LEAF_ENTRY_SIZE * (MAX_NODE_SIZE + 1); i >= j; i--) {
      buffer[i] = buffer[i - NON_LEAF_ENTRY_SIZE];
    }
  }

  //insert record
  char* p = (char*) &pid;
  for (int i = 0; i < KEY_SIZE; i++) {
    buffer[j] = (*p);
    p++;
    j++;
  }

  //insert key
  p = (char*) &key;
  for (int i = 0; i < KEY_SIZE; i++) {
    buffer[j] = (*p);
    p++;
    j++;
  }
  numKeys++; // End insert

  // Split the keys
  int middle = numKeys / 2 + 1;
  int left = numKeys / 2;            //1 - 35
  int right = (numKeys / 2) + 2;      //37 - 71

  // Copy right (middle.....right) side into sibling
  char* src = buffer + middle * NON_LEAF_ENTRY_SIZE;
  std::memcpy(sibling.buffer, src, (numKeys-middle) * NON_LEAF_ENTRY_SIZE);

  // Set midKey to the middle key
  midKey = middle;

  // update numKeys
  sibling.numKeys = numKeys - middle;
  numKeys = left;

  // Clear the 2nd half of the original node after the middle split
  char* begin = &buffer[middle * NON_LEAF_ENTRY_SIZE];
  char* end = begin + ((MAX_NODE_SIZE + 1 - middle) * NON_LEAF_ENTRY_SIZE);
  std::fill(begin, end, 0);

  return rc;
}

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid) {
  RC rc = 0;
  int numEntries = 0;
  int curEntry = 0;
  int key;
  PageId entry_pid;
  char* traverse = buffer;

  while (numEntries <= numKeys) {
      rc = readNonLeafEntry(curEntry, key, entry_pid);
      if (key == searchKey) {
          memcpy(&pid, traverse + (curEntry * NON_LEAF_ENTRY_SIZE) + KEY_SIZE, sizeof(PageId));
          return 0; // Found searchKey
      }
      else { // If greater, keep searching
          curEntry++;
          numEntries++;
      }
  }
  // If not found, return error
  rc = RC_NO_SUCH_RECORD;

  return rc;
}

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2) {
  char* p;
  p = (char*) &pid1;
  memcpy(&buffer, p, sizeof(PageId));

  p = (char*) &key;
  char* q = buffer;
  q += sizeof(PageId);
  memcpy(q, p, KEY_SIZE);

  p = (char*) &pid2;
  q += sizeof(key);
  memcpy(q, p, sizeof(PageId));

  return 0;
}