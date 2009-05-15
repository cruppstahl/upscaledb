/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 *
 * A simple example, which creates a database, inserts some values, 
 * looks them up and erases them. Uses the C++ api.
 */

#include <iostream>
#include <ham/hamsterdb.hpp>

typedef struct
{
 char trNumber[20]; // No.
} KeyTrain;

typedef struct
{
 char stNo[3];  // No.
 char stName[12]; // station name
 char stDist[6];  // current distance
 char stATime[6]; // arrive time
 char stLTime[6]; // leave time
 char stDays[2];  // which day
} RecTrainDetails;

#define MAX 7

int 
run_demo(void)
{
    ham::db db;            /* hamsterdb database object */
    ham::key key;          /* a key */
    ham::record record;    /* a record */
    ham::env env;
    ham::cursor cursor;

    int i;
    KeyTrain kTrain, trains[MAX];
    RecTrainDetails rTrainDetails, traindetails[MAX];

    memset(&trains[0], 0, sizeof(trains));
    memset(&traindetails[0], 0, sizeof(traindetails));

    strcpy(trains[0].trNumber, "D1");
    strcpy(trains[1].trNumber, "D1");
    strcpy(trains[2].trNumber, "T1");
    strcpy(trains[3].trNumber, "T1");
    strcpy(trains[4].trNumber, "T1");
    strcpy(trains[5].trNumber, "T1");
    strcpy(trains[6].trNumber, "T1");
    strcpy(traindetails[0].stNo, "1");
    strcpy(traindetails[0].stName, "XXXX");
    strcpy(traindetails[0].stDist, "0");
    strcpy(traindetails[1].stNo, "2");
    strcpy(traindetails[1].stName, "XXXXXX");
    strcpy(traindetails[1].stDist, "703");
    strcpy(traindetails[2].stNo, "1");
    strcpy(traindetails[2].stName, "XXXXX");
    strcpy(traindetails[2].stDist, "0");
    strcpy(traindetails[3].stNo, "2");
    strcpy(traindetails[3].stName, "XXXXXXXX");
    strcpy(traindetails[3].stDist, "277");
    strcpy(traindetails[4].stNo, "3");
    strcpy(traindetails[4].stName, "XXXXXXX");
    strcpy(traindetails[4].stDist, "689");
    strcpy(traindetails[5].stNo, "4");
    strcpy(traindetails[5].stName, "XX");
    strcpy(traindetails[5].stDist, "1225");
    strcpy(traindetails[6].stNo, "5");
    strcpy(traindetails[6].stName, "XXX");
    strcpy(traindetails[5].stDist, "1440");
 
    env.create("test.db");
    db = env.create_db(13, HAM_ENABLE_DUPLICATES);
    db.enable_compression(9); // compression
    cursor.create(&db);

    for (i=0; i<MAX; i++) {
        kTrain=trains[i];
        rTrainDetails=traindetails[i];
 
  key.set_size(sizeof(kTrain));
  key.set_data(&kTrain);
  try {
   // traverse the duplicates
   cursor.find(&key); // locate first
   while(1)
   {
    cursor.move(0,&record,0); // load current record
    if (!memcmp(((RecTrainDetails *)record.get_data())->stNo, rTrainDetails.stNo, 3))
    { // compare
     printf("already in.\n");
     break;
    }
    cursor.move(0,&record,HAM_CURSOR_NEXT|HAM_ONLY_DUPLICATES); // move to next
    // cursor.move(0,0,HAM_CURSOR_NEXT|HAM_ONLY_DUPLICATES); 
    // above will make error when reach the 3rd duplicate entry
   }
  }
  catch (ham::error &e) {
   if (e.get_errno() == -11) // not found or no duplicates
   {
    record.set_size(sizeof(rTrainDetails));
    record.set_data(&rTrainDetails);
    db.insert(&key,&record,HAM_DUPLICATE);
   }
   else
    throw(e);
  }
 }

    std::cout << "success!" << std::endl;
	return (0);
}

int 
main(int argc, char **argv)
{
    try {
        return (run_demo());
    }
    catch (ham::error &e) {
        std::cerr << "run_demo() failed with unexpected error "
                  << e.get_errno() << " ('"
                  << e.get_string() << "')" << std::endl;
        return (-1);
    }
}
