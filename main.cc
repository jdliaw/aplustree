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


    
    // printf("Start printing for pid1: ");
    // leaf.read(1, pf);
    // leaf.printStuff();
    
    // printf("Start printing for pid2: ");
    // leaf.read(2, pf);
    // leaf.printStuff();

    // printf("Start printing for pid3: ");
    // root.read(3, pf);
    // root.printStuff();
    
    // printf("Start printing for pid4: ");
    // leaf.read(4, pf);
    // leaf.printStuff();
    
    // printf("Start printing for pid5: ");
    // leaf.read(5, pf);
    // leaf.printStuff();

    // printf("Start printing for pid74: ");
    // root.read(74, pf);
    // root.printStuff();

    int i = 1;
    while(i != -1) {
      leaf.read(i, pf);
      leaf.printStuff();
      i = leaf.getNextNodePtr();
    }
    
//    leaf.readEntry(0, key, output);
//    printf("Pid: 1\t");
//    printf("Key: %d\t", key);
//    printf("Record pid: %d\t", output.pid);
//    printf("Record sid: %d\t", output.sid);
//    printf("Sibling Node: %d\n", leaf.getNextNodePtr());
//    
//    while(leaf.getNextNodePtr() >= 0) {
//        printf("Pid: %d\t", leaf.getNextNodePtr());
//        leaf.read(leaf.getNextNodePtr(), pf);
//        leaf.readEntry(0, key, output);
//        printf("Key: %d\t", key);
//        printf("Record pid: %d\t", output.pid);
//        printf("Record sid: %d\t", output.sid);
//        printf("Sibling Node: %d\n", leaf.getNextNodePtr());
//    }
//    
//    
//    BTNonLeafNode nonleaf;
//    PageId o_pid;
//    int curr_depth = 1;
//    queue<PageId> pids;
//    pids.push(root_pid);
//    
//    while (curr_depth < tree_height) {
//        queue<PageId> next_pids;
//        while (!pids.empty()) {
//            nonleaf.read(pids.front(), pf);
//            printf("Pid: %d\t", pids.front());
//            printf("Number of Keys: %d\t", nonleaf.getKeyCount());
////            nonleaf.readFirstPid(o_pid);
////            printf("First pid: %d\t", o_pid);
//            next_pids.push(o_pid);
//            nonleaf.readNonLeafEntry(0, key, o_pid);
//            printf("Key: %d\t", key);
//            printf("Second pid: %d\t", o_pid);
//            next_pids.push(o_pid);
//            for (int i = 1; i < nonleaf.getKeyCount(); i++) {
//                nonleaf.readNonLeafEntry(i, key, o_pid);
//                printf("Key: %d\t", key);
//                printf("Next pid: %d\t", o_pid);
//                next_pids.push(o_pid);
//            }
//            pids.pop();
//            printf("\n");
//        }
//        pids = next_pids;
//        curr_depth++;
//    }
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
    
    
  // index.close();
  //   //print function is called here
  // print_index(index.getRootPid(), index.getTreeHeight(), file);
  // run the SQL engine taking user commands from standard input (console).
  // SqlEngine::run(stdin);

  return 0;
}
