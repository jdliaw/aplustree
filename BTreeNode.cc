#include "BTreeNode.h"

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
  return pf.read(pid, &buffer);
}

/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf) {
  return pf.write(pid, &buffer);
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
  int j = eid * ENTRY_SIZE; // j = index to insert at

  // CASE: Inserting in the middle
  if (eid < numKeys) {
    // Move everything back by one entry
    for (int i = ENTRY_SIZE * MAX_NODE_SIZE; i >= j; i--) {
      buffer[i] = buffer[i - ENTRY_SIZE];
    }
  }

  // First append the key
  p = (char*) &key; // p == first byte of key
  for (int i = 0; i < KEY_SIZE; i++) {
    buffer[j] = (*p);
    p++;
    j++;
  }

  // Now append record id
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
  if (sibling is not empty) {
    return RC_INVALID_ATTRIBUTE; // TODO: what to return if sibling node is not empty?
  }

  // Do insert as before
  RC rc;
  int eid;
  rc = locate(key, eid);
  int j = eid * ENTRY_SIZE;

  if (eid < numKeys) {
    for (int i = ENTRY_SIZE * (MAX_NODE_SIZE + 1); i >= j; i--) {
      buffer[i] = buffer[i - ENTRY_SIZE];
    }
  }

  char* p = (char*) &key;
  for (int i = 0; i < KEY_SIZE; i++) {
    buffer[j] = (*p);
    p++;
    j++;
  }

  p = (char*) &rid.pid;
  for (int i = 0; i < RID_SIZE; i++) {
    if (i == 4) {
      p = (char*) &rid.sid;
    }
    buffer[j] = (*p);
    p++;
    j++;
  }
  numKeys++; // End insert

  // Split the keys
  int middle = (numKeys / 2) + 1; // How many go in the left node (not sibling)
  // start at eid = middle (bc index)
  RecordId siblingRID;
  RecordId rid;
  int key;

  // Insert records by eid from middle to last key (numKeys) into sibling
  for (int i = middle; i < numKeys; i++) {
    rc = readEntry(i, key, rid);
    sibling.insert(key, rid);

    // update first key of sibling
    if (i == middle) {
      siblingKey = key;
      siblingRID = rid;
    }
  }

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
  RC rc;
  int numEntries = 0;
  int curEntry = 0;
  int key;
  RecordId rid;

  while (numEntries <= numKeys) {
    rc = readEntry(curEntry, key, rid);
    if (searchKey == key) {
      eid = curEntry;
      return 0; // Found searchKey
    }
    else if (searchKey < key) { // If less, keep searching
      curEntry++;
      numEntries++;
    }
    else {
      // If greater, we want to return the entry right after searchKey
      eid = curEntry;
      return RC_NO_SUCH_RECORD;
    }
  }
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
  int entryIndex = eid * ENTRY_SIZE;
  for (int i = entryIndex; i <= entryIndex + ENTRY_SIZE; i += 4) {
    if (i == entryIndex) {
      key = (int)(buffer[i+3] << 24 | buffer[i+2] << 16 | buffer[i+1] << 8 | buffer[i]);
    }
    else if (i == entryIndex+4) {
      rid.pid = (int)(buffer[i+3] << 24 | buffer[i+2] << 16 | buffer[i+1] << 8 | buffer[i]);
    }
    else {
      rid.sid = (int)(buffer[i+3] << 24 | buffer[i+2] << 16 | buffer[i+1] << 8 | buffer[i]);
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
  return siblingID;
}

/*
 * Set the pid of the next slibling node.
 * @param pid[IN] the PageId of the next sibling node
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid) {
  pid = pid;
  return 0;
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{ return 0; }

/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{ return 0; }

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{ return 0; }


/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{ return 0; }

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
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{ return 0; }

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{ return 0; }

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{ return 0; }
