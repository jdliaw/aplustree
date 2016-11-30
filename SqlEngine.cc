/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeIndex.h"

using namespace std;

// external functions and variables for load file and sql command parsing
extern FILE* sqlin;
int sqlparse(void);


RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");

  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}

RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
  RecordFile rf;   // RecordFile containing the table
  RecordId   rid;  // record cursor for table scanning

  BTreeIndex index;
  RC     rc;
  int    key;
  string value;
  int    count;
  int    diff;

  // open the table file
  if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
    fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
    return rc;
  }

  rid.pid = rid.sid = 0;
  count = 0;

  bool hasVal = false;
  int min = -1;
  int max = -1;
  int findKey = -1;
  bool isNE = false;
  bool condEQ = false;
  bool indexOpened = false;

  // Determine our conditions (so can choose to use index or table)
  for (unsigned i = 0; i < cond.size(); i++) {
    if (cond[i].attr == 2) {
      hasVal = true;
    }

    int val = atoi(cond[i].value);
    switch (cond[i].comp) {
      case SelCond::EQ:
        findKey = atoi(cond[i].value);
        break;
      case SelCond::NE:
        isNE = true;
        break;
      case SelCond::LT:
        if (max == -1 || val > max)
          max = val;
        break;
      case SelCond::LE:
        if (max == -1 || val > max) {
          max = val;
          condEQ = true;
        }
        break;
      case SelCond::GT:
        if (min == -1 || val < min)
          min = val;
        break;
      case SelCond::GE:
        if (min == -1 || val < min) {
          min = val;
          condEQ = true;
        }
        break;
      default:
        break;
    }
  }

  string idx_file = table + ".idx";
  rc = index.open(idx_file.c_str(), 'r');

  // Check if conditions are possible
  if (min > max && max != -1 && min != -1) {
    goto exit_to_print;
  }
  if (min == max && !condEQ && max != -1 && min != -1) {
    goto exit_to_print;
  }

  /* DON'T use index tree when:
    1. Index tree doesn't exist
    2. Not Equal condition on key
  */

  if (rc != 0 || (isNE && !hasVal)) {
    // Use table

    // scan the table file from the beginning
    while (rid < rf.endRid()) {
      // read the tuple
      if ((rc = rf.read(rid, key, value)) < 0) {
        fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
        goto exit_select;
      }

      // check the conditions on the tuple
      for (unsigned i = 0; i < cond.size(); i++) {
        // compute the difference between the tuple value and the condition value
        switch (cond[i].attr) {
        case 1:
          diff = key - atoi(cond[i].value);
          break;
        case 2:
          diff = strcmp(value.c_str(), cond[i].value);
          break;
        default: break;
        }

        // skip the tuple if any condition is not met
        switch (cond[i].comp) {
        case SelCond::EQ:
          if (diff != 0) goto next_tuple;
          break;
        case SelCond::NE:
          if (diff == 0) goto next_tuple;
          break;
        case SelCond::GT:
          if (diff <= 0) goto next_tuple;
          break;
        case SelCond::LT:
          if (diff >= 0) goto next_tuple;
          break;
        case SelCond::GE:
          if (diff < 0) goto next_tuple;
          break;
        case SelCond::LE:
          if (diff > 0) goto next_tuple;
          break;
        default:
          break;
        }
      }

      // the condition is met for the tuple.
      // increase matching tuple counter
      count++;

      // print the tuple
      switch (attr) {
      case 1:  // SELECT key
        fprintf(stdout, "%d\n", key);
        break;
      case 2:  // SELECT value
        fprintf(stdout, "%s\n", value.c_str());
        break;
      case 3:  // SELECT *
        fprintf(stdout, "%d '%s'\n", key, value.c_str());
        break;
      }

      // move to the next tuple
      next_tuple:
      ++rid;
    }
  }
  else {
    // Use index
    IndexCursor cursor;
    indexOpened = true;

    if (findKey != -1 && !hasVal) {
      index.locate(findKey, cursor); // returns set cursor
    }
    else if (min != -1 && condEQ) {
      index.locate(min, cursor); // >= min
    }
    else if (min != -1 && !condEQ) {
      index.locate(min+1, cursor); // > min
    }
    else {
      index.locate(0, cursor); // start at root
    }

    // Read through index
    while (index.readForward(cursor, key, rid) == 0) {
      // If SELECT key or count(*), don't read from disk [attr 1 == key, 4 == count(*)]
      if (!hasVal && (attr == 1 || attr == 4)) {
        if (findKey != -1 && key != findKey) { // doesn't meet conditions
          goto exit_to_print;
        }
        if (max != -1 && ((condEQ && key >= max) || (!condEQ && key > max))) {
          goto exit_to_print;
        }
        if (min != -1 && ((condEQ && key <= min) || (!condEQ && key < min))) {
          goto exit_to_print;
        }
        // print the tuples that match conditions
        if (attr == 1) {
          fprintf(stdout, "%d\n", key);
        }
        count++;
        continue; // Skip the reading from the disk and continue the while loop
      }
      else {
        // hasVal, so need to read from disk
        if ((rc = rf.read(rid, key, value)) < 0) {
          fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
          goto exit_select;
        }
        // check the conditions on the tuple
        for (unsigned i = 0; i < cond.size(); i++) {
          // compute the difference between the tuple value and the condition value
          switch (cond[i].attr) {
            case 1:
              diff = key - atoi(cond[i].value);
              break;
            case 2:
              diff = strcmp(value.c_str(), cond[i].value);
              break;
          }

          // skip the tuple if any condition is not met
          switch (cond[i].comp) {
            case SelCond::EQ:
              if (diff != 0) {
                if (cond[i].attr == 1) {
                  // Since keys are sorted, all tuples after won't match
                  goto exit_to_print;
                }
                // cout << "continue" << endl;
                goto continue_while_loop; // If not key, then go on to next
              }
              break;
            case SelCond::NE:
              if (diff == 0) goto continue_while_loop;
              break;
            case SelCond::GT:
              if (diff <= 0) goto continue_while_loop;
              break;
            case SelCond::LT:
              if (diff >= 0) {
                if (cond[i].attr == 1) {
                  // Keys are sorted, so rest of keys will be greater
                  goto exit_to_print;
                }
                goto continue_while_loop;
              }
              break;
            case SelCond::GE:
              if (diff < 0) goto continue_while_loop;
              break;
            case SelCond::LE:
              if (diff >= 0) {
                if (cond[i].attr == 1) {
                  // Keys are sorted, so rest of keys will be greater
                  goto exit_to_print;
                }
                goto continue_while_loop;
              }
              break;
          }
        }

        // the condition is met for the tuple.
        // increase matching tuple counter
        count++;

        // print the tuple
        switch (attr) {
        case 1:  // SELECT key
          fprintf(stdout, "%d\n", key);
          break;
        case 2:  // SELECT value
          fprintf(stdout, "%s\n", value.c_str());
          break;
        case 3:  // SELECT *
          fprintf(stdout, "%d '%s'\n", key, value.c_str());
          break;
        }

        continue_while_loop:
          ;
      }
    }
  }

  // print matching tuple count if "select count(*)"
  exit_to_print: // exit when conditions are no longer met
  if (attr == 4) {
    fprintf(stdout, "%d\n", count);
  }
  rc = 0;

  // close the table file and return
  exit_select:
  rf.close();
  if (indexOpened) {
    index.close();
  }
  return rc;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
  RC rc;
  // Create RecordFile named table.tbl in working directory
  string tablename = table + ".tbl";
  RecordFile rf(tablename, 'w'); // Will create and open RF

  // Create BTreeIndex & file for index named table.idx in working directory
  BTreeIndex idx;
  if (index) {
    string indexname = table + ".idx";
    rc = idx.open(indexname, 'w');
  }

  RecordId rid;
  int key;
  string value;

  // Open input file with fstream
  string in;
  ifstream f (loadfile.c_str());

  if (f.is_open()) {
    while (getline(f, in)) {
      // Read tuple from input file use parseLoadLine
      parseLoadLine(in, key, value);
      rc = rf.append(key, value, rid);
      // If index is true, insert corresponding tuple into B+Tree Index
      if (index) {
        rc = idx.insert(key, rid);
      }
    }
    f.close();
    if (index) {
      rc = idx.close(); // Close the file when done inserting index
    }
  }

  return rc; // Come back to how to implement RC
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
    const char *s;
    char        c;
    string::size_type loc;

    // ignore beginning white spaces
    c = *(s = line.c_str());
    while (c == ' ' || c == '\t') { c = *++s; }

    // get the integer key value
    key = atoi(s);

    // look for comma
    s = strchr(s, ',');
    if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

    // ignore white spaces
    do { c = *++s; } while (c == ' ' || c == '\t');

    // if there is nothing left, set the value to empty string
    if (c == 0) {
        value.erase();
        return 0;
    }

    // is the value field delimited by ' or "?
    if (c == '\'' || c == '"') {
        s++;
    } else {
        c = '\n';
    }

    // get the value string
    value.assign(s);
    loc = value.find(c, 0);
    if (loc != string::npos) { value.erase(loc); }

    return 0;
}
