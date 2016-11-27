/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeIndex.h"
#include "BTreeNode.h"

#include <cstdio>
#include <iostream>

void BTLeafTest() {
  printf("Starting BTLeafNodeTest...\n");
	BTLeafNode node;
  RC rc;
  int i;
  for(i = 0; i < 60; i++) {
    int key = i;
    RecordId rid;
    rid.pid = i;
    rid.sid = i;
    rc = node.insert(key, rid);
    if (rc != 0) {
		  std::cout << "error inserting key into node, " << i << std::endl;
    }
	}
  node.printStuff();
  for(; i < 90; i++) {
    int key = i;
    RecordId rid;
    rid.pid = i;
    rid.sid = i;
    rc = node.insert(key, rid);
    if (rc != 0) {
		  std::cout << "error inserting key into node, " << i << std::endl;
    }
	}
  BTLeafNode sibling;
  RecordId rid;
  rid.pid = 100;
  rid.sid = 100;
  int siblingKey;
  node.printStuff();
  rc = node.insertAndSplit(100, rid, sibling, siblingKey);
  node.printStuff();
  sibling.printStuff();
}

void BTNonLeafTest() {
  printf("Starting BTNonLeafNodeTest...\n");
	BTNonLeafNode node;
  RC rc;
  int i;
  for(i = 0; i < 60; i++) {
    int key = i;
    PageId pid;
    pid = i;
    rc = node.insert(key, pid);
    if (rc != 0) {
		  std::cout << "error inserting key into node, " << i << std::endl;
    }
	}
  for(; i < 90; i++) {
    int key = i;
    PageId pid;
    pid = i;
    rc = node.insert(key, pid);
    if (rc != 0) {
		  std::cout << "error inserting key into node, " << i << std::endl;
    }
	}
  BTNonLeafNode sibling;
  int key = 100;
  PageId pid = 100;
  int midKey;
  rc = node.insertAndSplit(key, pid, sibling, midKey);
  std::cout << "midKey: " << midKey << std::endl;

  node.printStuff();
  sibling.printStuff();
}

int main()
{
//  BTLeafTest():
  BTNonLeafTest();  

  // run the SQL engine taking user commands from standard input (console).
  // SqlEngine::run(stdin);

  return 0;
}
