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
  for(i = 0; i < 60; i+=2) {
    int key = i;
    PageId pid;
    pid = i;
    rc = node.insert(key, pid);
    if (rc != 0) {
		  std::cout << "error inserting key into node, " << i << std::endl;
    }
	}
  for(int i = 1; i < 61; i+=2) {
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


void print_index(PageId root_pid, int tree_height, std::string idx) {
    int key;
    PageFile pf;
    pf.open(idx, 'w');	// TODO: change this to the name of your index file

    BTLeafNode leaf;
    BTNonLeafNode root;
    RecordId output;




    int i = 1;
    while(i != -1) {
      leaf.read(i, pf);
      leaf.printStuff();
      i = leaf.getNextNodePtr();
    }


    root.read(2660, pf);
    root.printStuff();
    pf.close();
}

void insertStuff(BTreeIndex &index) {
    RecordId rid;
    int i;
    for(i = 0; i < 80; i+= 2) {
        rid.pid = i;
        rid.sid = i;
        index.insert(i, rid);
    }

    for(i = 5; i < 85; i+=2) {
        rid.pid = i;
        rid.sid = i;
        index.insert(i, rid);
    }

    for(; i < 143; i++) {
        rid.pid = i;
        rid.sid = i;
        index.insert(i, rid);
    }


    for(; i < 150; i++) {
        rid.pid = i;
        rid.sid = i;
        index.insert(i, rid);
    }

    rid.pid = 3;
    rid.sid = 3;
    index.insert(3, rid);
    for(i = 200; i < 300; i+=2) {
        rid.pid = i;
        rid.sid = i;
        index.insert(i, rid);
    }
    for(i = 201; i < 301; i+=2) {
        rid.pid = i;
        rid.sid = i;
        index.insert(i, rid);
    }

    for(i = 302; i < 320; i+=2) {
        rid.pid = i;
        rid.sid = i;
        index.insert(i, rid);
    }

    for(; i < 5000; i+=2) {
        rid.pid = i;
        rid.sid = i;
        index.insert(i, rid);
    }

    for(i = 303; i < 385; i+=2) {
        rid.pid = i;
        rid.sid = i;
        index.insert(i, rid);
    }

    rid.pid = i;
    rid.sid = i;
    index.insert(i, rid);
    for(; i < 50001; i+=2) {
        rid.pid = i;
        rid.sid = i;
        index.insert(i, rid);
    }
    for(i = 10000; i < 100000; i+= 2) {
        rid.pid = i;
        rid.sid = i;
        index.insert(i, rid);
    }
    for(; i < 200000; i+= 1) {
        rid.pid = i;
        rid.sid = i;
        index.insert(i, rid);
    }
}


int main()
{
//  BTLeafTest():
  // BTNonLeafTest();
  // BTreeIndex index;

  // std::string file = "test1.idx";
  // index.open(file, 'w');	// TODO: change this to the name of your index file

  // insertStuff(index);

  // printf("RootPid: %d\n", index.getRootPid());
  // printf("TreeHeight: %d\n", index.getTreeHeight());


  // IndexCursor cursor;
  // index.locate(50197, cursor);
  // std::cout << "cursor.pid: " << cursor.pid << ", cursor.eid: " << cursor.eid << std::endl;

  // index.close();

  // //   //print function is called here
  //  print_index(index.getRootPid(), index.getTreeHeight(), file);
  // run the SQL engine taking user commands from standard input (console).
  SqlEngine::run(stdin);

  return 0;
}
