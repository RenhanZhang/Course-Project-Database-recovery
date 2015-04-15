#include "LogMgr.h"
#include <sstream>
using namespace std;

  /*
   * Find the LSN of the most recent log record for this TX.
   * If there is no previous log record for this TX, return 
   * the null LSN.
   */
int LogMgr::getLastLSN(int txnum){
    map<int, txTableEntry>::iterator it = tx_table.find(txnum);
    if(it == tx_table.end())
      return NULL_LSN;
    else
      return it->second.lastLSN;
}


/*
 * Update the TX table to reflect the LSN of the most recent
 * log entry for this transaction.
 */
void LogMgr::setLastLSN(int txnum, int lsn){
    // if the tx is already in the table, update the lastLSN
    // else create a new entry
    if(tx_table.find(txnum) != tx_table.end())
        tx_table[txnum].lastLSN = lsn;
    else
        tx_table[txnum] = txTableEntry {lsn, U};
}

/*
 * Force log records up to and including the one with the
 * maxLSN to disk. Don't forget to remove them from the
 * logtail once they're written!
 */
void LogMgr::flushLogTail(int maxLSN){
    string logStr = "";
    if(logtail[0]->getLSN() > maxLSN)
        return;

    int i = 0;
    for (; i < logtail.size(); i++){
        int thisLSN = logtail[i]->getLSN();
        logStr += logtail[i]->toString();
        if(thisLSN == maxLSN)
          break;
    }

    logtail.erase(logtail.begin(), logtail.begin()+i+1);
    se->updateLog(logStr);
}

/*
 * Run the analysis phase of ARIES.
 */
void LogMgr::analyze(vector <LogRecord*> log){

  int i = log.size()-1;
  //int masterLSN = se->get_master();
    
  // find the most recent checkpoint by looking at masterLSN
  for(; i >= 0; --i){
    if(log[i]->getType() == END_CKPT){
      ChkptLogRecord* lr = (ChkptLogRecord*) log[i];
      tx_table = lr->getTxTable();
      dirty_page_table = lr->getDirtyPageTable();
      break;
    }
  }
  i = i < 0? 0:i;
    
  for(; i < log.size(); ++i){
    
    int thisLSN = log[i]->getLSN();
    int thisTxId = log[i]->getTxID();
    TxType type = log[i]->getType();

    // lastLSN
    if(type == UPDATE ||type == COMMIT ||type == ABORT ||type == CLR)
      setLastLSN(thisTxId, thisLSN);

    if(type == END)
      tx_table.erase(thisTxId);
    else if(type == COMMIT)
      tx_table[thisTxId].status = C;


    // dirty page table for undoable action: UPDATE, CLR
    if(type == UPDATE){
      UpdateLogRecord* lr = (UpdateLogRecord*) log[i];
      if(dirty_page_table.find(lr->getPageID()) == dirty_page_table.end())
          dirty_page_table[lr->getPageID()] = thisLSN;
    }

    if(type == CLR){
      CompensationLogRecord* lr = (CompensationLogRecord*) log[i];
      if(dirty_page_table.find(lr->getPageID()) == dirty_page_table.end())
          dirty_page_table[lr->getPageID()] = thisLSN;
    }


  }
}


/*
* Run the redo phase of ARIES.
* If the StorageEngine stops responding, return false.
* Else when redo phase is complete, return true. 
*/
bool LogMgr::redo(vector <LogRecord*> log){

     //find the start point of the redo process
     int start_lsn = dirty_page_table.begin()->second;      
     for(map<int,int>:: iterator it = dirty_page_table.begin(); it != dirty_page_table.end(); it++)
        if(it->second < start_lsn)
           start_lsn = it->second;

     for(int i = 0; i < log.size(); i++){
          int thisLSN = log[i]->getLSN();
          if (thisLSN < start_lsn)
               continue;
          TxType type = log[i]->getType();

          /* if update, avoid undoing if any of the 3 holds: 
          (1) the page is not dirty
          (2) the page is not dirty, but the recLSN > thisLSN
          (3) pageLSN is greater than thisLSN

          if needs undoing, do: 
          (4): reapply the action
          (5): change the pageLSN as thisLSN
          */
          if(type == UPDATE || type == CLR){
               int pg_id = -1;

               if(type ==  UPDATE){
                    UpdateLogRecord* ulr = (UpdateLogRecord*) log[i];
                    pg_id = ulr->getPageID();       // id of the page 
               }else{
                    CompensationLogRecord* clr = (CompensationLogRecord*) log[i];
                    pg_id = clr->getPageID();
               }
                         
               // see if it's in the dirty_page_table
               map<int,int>::iterator it = dirty_page_table.find(pg_id);

               if(it == dirty_page_table.end())        // (1)
                    continue;

               int recLSN = it->second;
               if (recLSN > thisLSN)
                    continue;                          //(2)

               if(se->getLSN(pg_id) > thisLSN)         //(3)
                    continue;
               
               bool success = false;
               if(type == UPDATE){                                              //(4)
                    UpdateLogRecord* ulr = (UpdateLogRecord*) log[i];
                    success = se->pageWrite(pg_id, ulr->getOffset(), ulr->getAfterImage(), thisLSN);
               }else{
                    CompensationLogRecord* clr = (CompensationLogRecord*) log[i];
                    success = se->pageWrite(pg_id, clr->getOffset(), clr->getAfterImage(), thisLSN);
               }

               if(!success)
                    return false;
          }
          else if(type == COMMIT){
               setLastLSN(log[i]->getTxID(), thisLSN);
               tx_table[log[i]->getTxID()].status = C;
          }
          else if (type == END)
                tx_table.erase(log[i]->getTxID()); 
          
     }
     
     for(map<int, txTableEntry>::iterator it = tx_table.begin(); it != tx_table.end(); it++){
         if((it->second).status != C)
             continue;
         logtail.push_back(new LogRecord(se->nextLSN(), (it->second).lastLSN, it->first, END)); 
         tx_table.erase(it->first); 
     }

     return true;
}


/*
 * If no txnum is specified, run the undo phase of ARIES.
 * If a txnum is provided, abort that transaction.
 * Hint: the logic is very similar for these two tasks!
 */
void LogMgr::undo(vector <LogRecord*> log, int txnum){
//void LogMgr::undo(vector <LogRecord*> log, int txnum = NULL_TX){

     // construct the toUndo set, value:LSN
     map<int, bool> toUndo;
     if(txnum == NULL_TX){
         for(map<int, txTableEntry>::iterator it = tx_table.begin(); it != tx_table.end(); it++)
             if((it->second).status != C)
                 toUndo[(it->second).lastLSN] = true;
     }
     else{
         if(tx_table[txnum].status != C)
             toUndo[tx_table[txnum].lastLSN] = true;
         int abort_lsn = se->nextLSN();
         logtail.push_back(new LogRecord(abort_lsn, getLastLSN(txnum), txnum, ABORT));
         //logtail.push_back(new LogRecord(abort_lsn, 222222, txnum, ABORT));
         setLastLSN(txnum, abort_lsn);
     }

     for(int i = log.size()-1; i >= 0; i--){

          if(toUndo.empty()) break;
              
          // if the LSN is not found in the toUndo set, skip
          int thisLSN = log[i]->getLSN();
          if(toUndo.find(thisLSN) == toUndo.end())
               continue;
          int txid = log[i]->getTxID();
          TxType type = log[i]->getType();

          /* if it's update, do:
             (1): write the before image to the page
             (2): append a CLR to the logtail and update tx_table, dirty_page_table
             (3): remove this LSN from the toUndo
             (4): change the lastLSN for this transaction
             if this is the first action of this tx:
                  (5) end this tx: add an end lr
                  (6) remove this tx from tx_table
             else:
                  (7): add its prev LSN to toUndo
          */
          if(type == UPDATE){
             UpdateLogRecord* ulr = (UpdateLogRecord*) log[i];
             int pg_id = ulr->getPageID();
             if(!(se->pageWrite(pg_id, ulr->getOffset(), ulr->getBeforeImage(), thisLSN)))   //(1)
                 return;
            
             int clrLSN = se->nextLSN();
             CompensationLogRecord* clr = new CompensationLogRecord(clrLSN, getLastLSN(txid), txid, ulr->getPageID(), ulr->getOffset(), ulr->getBeforeImage(), ulr->getprevLSN());
             logtail.push_back(clr);                                                               //(2)
             setLastLSN(txid, clr->getLSN());

             map<int,int>::iterator it = dirty_page_table.find(pg_id);
             if(it == dirty_page_table.end())
                   dirty_page_table[pg_id] = clrLSN;

             toUndo.erase(thisLSN);                                                                //(3)                                                              //(4)         
             tx_table[ulr->getTxID()].lastLSN = clrLSN;                                            //(4)    
             if(ulr->getprevLSN() == NULL_TX){
                  LogRecord* elr = new LogRecord(se->nextLSN(), getLastLSN(txid), txid, END);
                  logtail.push_back(elr);                                                          //(5)
                  tx_table.erase(txid);                                                            //(6)
             }else
                  toUndo[ulr->getprevLSN()] = true;                                                //(7)                               
          }

          /* (1) remove thisLSN from toUndo
             if it's a CLR and the undoNextLSN != NULL_LSN:
                (2) add the prevLSN to toUndo
             else
                (3) add an end lr for it
                (4) remove it from tx_table
             
          */
          else if(type == CLR){
             toUndo.erase(thisLSN);                                           //(1)
             CompensationLogRecord* clr = (CompensationLogRecord*) log[i];
             if(clr->getUndoNextLSN() != NULL_LSN)
                 toUndo[clr->getUndoNextLSN()] = true;                        //(2)
             else{
                 int txid = log[i]->getTxID();
                 LogRecord* elr = new LogRecord(se->nextLSN(), getLastLSN(txid), txid, END);
                 logtail.push_back(elr);                                                          //(3)
                 tx_table.erase(log[i]->getTxID());                                               //(4)
             }
          }
     }

}


vector<LogRecord*> LogMgr::stringToLRVector(string logstring){
    vector<LogRecord*> result;
    istringstream stream(logstring);
    string line;
    while(getline(stream, line)){
      LogRecord* lr = LogRecord::stringToRecordPtr(line);
      result.push_back(lr);
    }
    return result;

}

/*
 * Abort the specified transaction.
 * Hint: you can use your undo function
 */
void LogMgr::abort(int txid){
     /* all_logs consists of two parts: logs on disk and logs in the log tail*/
     vector<LogRecord*> all_logs = stringToLRVector(se->getLog());
     all_logs.insert(all_logs.end(), logtail.begin(), logtail.end());
     undo(all_logs, txid);
}

/*
 * Write the begin checkpoint and end checkpoint
 */
void LogMgr::checkpoint(){
     /* do the following:
     (1) write begin ckpt
     (2) write end ckpt
     (3) flush logtail
     (4) write master_lsn
     */
     int bg_ckpt_lsn = se->nextLSN();
     LogRecord* begin_lr = new LogRecord(bg_ckpt_lsn, NULL_LSN, NULL_TX, BEGIN_CKPT);
     logtail.push_back(begin_lr);        //(1)

     int end_ckpt_lsn = se->nextLSN();
     ChkptLogRecord* ckpt = new ChkptLogRecord(end_ckpt_lsn, bg_ckpt_lsn, NULL_TX, tx_table, dirty_page_table);
     logtail.push_back(ckpt);  //(2)

     flushLogTail(end_ckpt_lsn);           //(3)
     
     se->store_master(bg_ckpt_lsn); //(4)
}


/*
 * Commit the specified transaction.
 */
void LogMgr::commit(int txid){
     /* (1) write a commit lr and update tx_table
        (2) flush logtail 
        (3) write an end lr
        (4) remove it from tx_table
     */

     int commit_lsn = se->nextLSN();
     LogRecord* commit_lr = new LogRecord(commit_lsn, getLastLSN(txid), txid, COMMIT);
     logtail.push_back(commit_lr);      //(1)
     setLastLSN(txid, commit_lsn);

     flushLogTail(commit_lsn);          //(2)

     int end_lsn = se->nextLSN();
     LogRecord* end_lr = new LogRecord(end_lsn, commit_lsn, txid, END);
     logtail.push_back(end_lr);                   //(3)
 
     tx_table.erase(txid);                        //(4)function checkpoint() and end of that function? How many
}

/*
 * A function that StorageEngine will call when it's about to 
 * write a page to disk. 
 * Remember, you need to implement write-ahead logging
 */
void LogMgr::pageFlushed(int page_id){

     /* do: (1) flush the logtail
            (2) remove the page from dirty_page_table
     */
      flushLogTail(se->getLSN(page_id));    //(1)
      dirty_page_table.erase(page_id);      //(2)

}


void LogMgr::recover(string log){
     vector<LogRecord*> v = stringToLRVector(log);
     analyze(v);
     if(redo(v))
        undo(v);
}


/*
 * Logs an update to the database and updates tables if needed.
 */
int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext){
    /* (1) change dirty_page_table and tx_table
       (2) append a logrecord to the logtail
    */
    int thisLSN = se->nextLSN();

    map<int, int>::iterator it = dirty_page_table.find(page_id);
    if(it == dirty_page_table.end())
       dirty_page_table[page_id] = thisLSN;                 //(1)

    int prevLSN = getLastLSN(txid);
  
    UpdateLogRecord* temp_ulr = new UpdateLogRecord(thisLSN, prevLSN, txid, page_id, offset, oldtext, input);
    setLastLSN(txid, thisLSN);
    logtail.push_back(temp_ulr);                      //(2)

    return thisLSN;
    
}

  /*
   * Sets this.se to engine. 
   */
void LogMgr::setStorageEngine(StorageEngine* engine){
  se = engine;
}







