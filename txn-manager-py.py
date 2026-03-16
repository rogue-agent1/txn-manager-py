#!/usr/bin/env python3
"""Transaction manager with ACID guarantees (in-memory). #360 MILESTONE"""
import sys,copy

class TransactionManager:
    def __init__(self):self.data={};self._txn_id=0;self.active={};self.log=[]
    def begin(self):
        self._txn_id+=1;tid=self._txn_id
        self.active[tid]={"snapshot":copy.deepcopy(self.data),"writes":{},"status":"active"}
        self.log.append(("BEGIN",tid))
        return tid
    def read(self,tid,key):
        txn=self.active[tid]
        if key in txn["writes"]:return txn["writes"][key]
        return txn["snapshot"].get(key)
    def write(self,tid,key,value):
        self.active[tid]["writes"][key]=value
        self.log.append(("WRITE",tid,key,value))
    def commit(self,tid):
        txn=self.active[tid]
        for k,v in txn["writes"].items():
            self.data[k]=v
        txn["status"]="committed"
        self.log.append(("COMMIT",tid))
        del self.active[tid]
    def rollback(self,tid):
        self.active[tid]["status"]="aborted"
        self.log.append(("ROLLBACK",tid))
        del self.active[tid]
    def get(self,key):return self.data.get(key)

def main():
    if len(sys.argv)>1 and sys.argv[1]=="--test":
        tm=TransactionManager()
        # Basic ACID
        t1=tm.begin();tm.write(t1,"x",10);tm.write(t1,"y",20)
        assert tm.get("x") is None  # not committed yet
        tm.commit(t1)
        assert tm.get("x")==10 and tm.get("y")==20
        # Isolation
        t2=tm.begin();t3=tm.begin()
        tm.write(t2,"x",99)
        assert tm.read(t3,"x")==10  # sees snapshot
        tm.commit(t2)
        assert tm.read(t3,"x")==10  # still snapshot
        tm.commit(t3)
        assert tm.get("x")==99
        # Rollback
        t4=tm.begin();tm.write(t4,"z",42);tm.rollback(t4)
        assert tm.get("z") is None
        # Log
        assert len(tm.log)>0
        assert tm.log[0]==("BEGIN",1)
        print("All tests passed! 🎉 #360 MILESTONE")
    else:
        tm=TransactionManager()
        t=tm.begin();tm.write(t,"balance",1000);tm.commit(t)
        print(f"balance = {tm.get('balance')}")
if __name__=="__main__":main()
