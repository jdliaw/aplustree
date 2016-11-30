/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include "BTreeIndex.h"
#include "BTreeNode.h"
#include <iostream>
#include <cstring>

using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
  rootPid = -1;
  treeHeight = 0;
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)
{
  RC rc;
  rc = pf.open(indexname, mode);
  if (rc != 0) {
    return rc;
  }

  rc = pf.read(0, buffer); // use pid = 0 for reading rootPid/treeHeight from disk
  if (rc != 0) {
    return rc;
  }

  PageId pid;
  int height;

  memcpy(&pid, buffer, sizeof(PageId));
  memcpy(&height, buffer + sizeof(PageId), sizeof(int));

  if (pid > rootPid) { // set rootPid and treeHeight
    rootPid = pid;
  }

  if (height > treeHeight) {
    treeHeight = height;
  }

  return 0;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
  RC rc;

  memcpy(buffer, &rootPid, sizeof(PageId));
  memcpy(buffer + sizeof(PageId), &treeHeight, sizeof(int));
  
  rc = pf.write(0, buffer); // Write rootPid/treeHeight to disk

  rc = pf.close();
  if (rc != 0) {
    return rc;
  }

  return 0;
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
  RC rc;
  // Empty tree, insert first element
  BTLeafNode leaf_node;

  if (treeHeight == 0) {
    rc = leaf_node.insert(key, rid);
    treeHeight++;
    rootPid = 1; // pid 0 is saved for index stf

    rc = leaf_node.write(rootPid, pf); // write before done
    return rc;
  }

  // IndexCursor cursor;
  // rc = locate(key, cursor);
  // // cursor is now at entry immediately after largest key smaller than searchKey

  PageId movePid = -1;
  int moveKey = -1;
  rc = insertHelper(key, rid, rootPid, 1, movePid, moveKey); // start at root (height == 1)

  return 0;
}

RC BTreeIndex::insertHelper(int key, const RecordId& rid, PageId curPid, int curHeight, PageId& movePid, int& moveKey) {
  RC rc;
    movePid = -1;
    moveKey = -1;
  if (curHeight == treeHeight) { // Base case: inserting leaf node
    BTLeafNode leaf_node;
    rc = leaf_node.read(curPid, pf);
    if (rc != 0) {
      return rc;
    }
    rc = leaf_node.insert(key, rid);
    if (rc == 0) { // If successfully insert, write and return (no overflow)
      rc = leaf_node.write(curPid, pf);
      return rc;
    }
    // Not successfully insert. Overflow => try insertAndSplit
    BTLeafNode siblingLeaf;
    int siblingKey;
    rc = leaf_node.insertAndSplit(key, rid, siblingLeaf, siblingKey); // returns siblingKey, which we need to push to parent

    if (rc != 0) {
      return rc;
    }
    // else successful
    int endPid = pf.endPid();
    movePid = endPid; // last pid in the current node points to the newly allocated node (sibling), which we want the parent to have
    moveKey = siblingKey; // sibling key needs to be pushed up to parent

    // set sibling next ptr
    siblingLeaf.setNextNodePtr(leaf_node.getNextNodePtr()); //TODO properly set this when we dont split the last node
    leaf_node.setNextNodePtr(endPid);

    // write to disk
    rc = leaf_node.write(curPid, pf);
    if (rc != 0) {
      return rc;
    }
    rc = siblingLeaf.write(endPid, pf);
    if (rc != 0) {
      return rc;
    }

    // at height of 1, insertAndSplit needs to create a new root to push up to
    if (treeHeight == 1) {
      BTNonLeafNode root;
        int root_key;
        RecordId root_rid;
        leaf_node.readEntry(0, root_key, root_rid); // ***
      rc = root.initializeRoot(curPid, root_key, endPid, siblingKey);
      treeHeight++;
      rootPid = pf.endPid(); // Write new root to the next empty spot in pf
      rc = root.write(rootPid, pf);
    }

    return rc;
  }
  else {  // Recursive case: inserting in middle
    BTNonLeafNode node;
    rc = node.read(curPid, pf);

    PageId childPid = -1;
    rc = node.locateChildPtr(key, childPid); // returns childPid TODO: something now working here

    int mPid = -1;
    int mKey = -1;
    rc = insertHelper(key, rid, childPid, curHeight+1, mPid, mKey); // Recursively traverse down the tree, following the ptrs
    
    // Once movePid & moveKey modified, (base case reached), can push them up to parent
    if (mPid != -1 && mKey != -1) {
      // Parent = our current node right now. Insert into cur.
      rc = node.insert(mKey, mPid);

      if (rc == 0) { // successfully insert, write & return
        rc = node.write(curPid, pf);
        return rc;
      }
      // Not successful, try insertAndSplit
      BTNonLeafNode siblingNode;
      int siblingKey;
      rc = node.insertAndSplit(mKey, mPid, siblingNode, siblingKey);

      if (rc != 0) {
        return rc; // error
      }

      int endPid = pf.endPid();
      movePid = endPid;
      moveKey = siblingKey; // push up again

      // write to disk
      rc = node.write(curPid, pf);
      if (rc != 0) {
        return rc;
      }
      rc = siblingNode.write(endPid, pf);
      if (rc != 0) {
        return rc;
      }

      // If push all the way to height == 1, need to make a new root again
      if (curHeight == 1) {
        BTNonLeafNode root;
          int root_key;
          PageId root_pid;
          node.readNonLeafEntry(0, root_key, root_pid); // ***
          rc = root.initializeRoot(curPid, root_key, endPid, siblingKey);
        treeHeight++;
        rootPid = pf.endPid(); // Write new root to the next empty spot in pf
        rc = root.write(rootPid, pf);
      }
    }
  }
  return rc;
}

/**
 * Run the standard B+Tree key search algorithm and identify the
 * leaf node where searchKey may exist. If an index entry with
 * searchKey exists in the leaf node, set IndexCursor to its location
 * (i.e., IndexCursor.pid = PageId of the leaf node, and
 * IndexCursor.eid = the searchKey index entry number.) and return 0.
 * If not, set IndexCursor.pid = PageId of the leaf node and
 * IndexCursor.eid = the index entry immediately after the largest
 * index key that is smaller than searchKey, and return the error
 * code RC_NO_SUCH_RECORD.
 * Using the returned "IndexCursor", you will have to call readForward()
 * to retrieve the actual (key, rid) pair from the index.
 * @param key[IN] the key to find
 * @param cursor[OUT] the cursor pointing to the index entry with
 *                    searchKey or immediately behind the largest key
 *                    smaller than searchKey.
 * @return 0 if searchKey is found. Othewise an error code
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{
  RC rc;
  BTNonLeafNode node;
  BTLeafNode leaf_node;

  PageId pid = rootPid;
  int eid;

  for (int height = 1; height < treeHeight; height++) {
    rc = node.read(pid, pf);
    if (rc != 0) {
      return rc;
    }

    rc = node.locateChildPtr(searchKey, pid);
    if (rc != 0 && rc != RC_NO_SUCH_RECORD) {
      return rc;
    }
  }

  // Now we are at the leaf level (height == treeHeight)
  rc = leaf_node.read(pid, pf);
  if (rc != 0) {
    return rc;
  }

  rc = leaf_node.locate(searchKey, eid);
  cursor.pid = pid;
  cursor.eid = eid;

  return rc;
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)
{
  RC rc;
  PageId pid = cursor.pid;
  int eid = cursor.eid;

  BTLeafNode node;
  rc = node.read(pid, pf);
  if (rc != 0) {
    return rc;
  }

  rc = node.readEntry(eid, key, rid);
  if (rc != 0) {
    return rc;
  }

  // If eid > numKeys, overflow. Find next node to move the cursor
  if (eid >= node.getKeyCount() - 1) {
    cursor.pid = node.getNextNodePtr();
    cursor.eid = 0;
  }
  else {
    cursor.eid++;
  }

  return 0;
}

// TODO: add these functions to your BTreeIndex.cc file for testing for the print function
int BTreeIndex::getTreeHeight(void) { return treeHeight; }
PageId BTreeIndex::getRootPid(void) { return rootPid; }
