# -*- coding: utf-8 -*-

'''delete duplicate pictures and videos by content
for videos only the first frame is compared
-the oldest file is kept, the newer one is deleted'''

import numpy as np
import cv2
import pandas as pd
import hashlib
import os,time
import multiprocessing as mp
from queue import Queue
import sys

def gethash(root,f,lf,i,q):
    try:
        fl=os.path.join(root,f)
        dt=os.path.getmtime(fl)
        if lf.endswith('.mp4'):
            cap = cv2.VideoCapture(fl)
            ret, frame = cap.read()
            cap.release()
        if lf.endswith('.jpg'):
            ret=True
            frame=cv2.imread(fl,0)
        sz=os.path.getsize(fl)
        if ret is True and frame is not None:
            hsh=hashlib.md5(frame).hexdigest()
            #q.put(f+' '+hsh)
            q.put((fl,hsh,dt,sz))
            #q.put(pd.DataFrame([[fl,hsh,dt,sz]],columns=['filename','hash','date','size']))
            #df=df.append(pd.DataFrame([[fl,hsh,dt,sz]],columns=['filename','hash','date','size']),ignore_index=True)
    except:print('some error')


start=time.time()
if __name__ == '__main__':
    try:mp.set_start_method('spawn')
    except:None
    delete=False
    hsh=None
    df=pd.DataFrame()
    rootdir=os.path.dirname(os.path.realpath(__file__))
    rootdir='e:\\sorted_pictures'
    prs=list()
    queue = mp.Queue(1000)
    cnt=0
    for root, subdirs, files in os.walk(rootdir):
        #print()
        #print(root,end='') 
        for i,f in enumerate(files):
            lf=f.lower()
            if lf.endswith('.mp4') or lf.endswith('.jpg'):
                print(lf)
                #gethash(root,f,lf,i,queue)
                p = mp.Process(target=gethash, args=(root,f,lf,i,queue))
                p.start()
                prs.append(p)
                if len(prs)>10:
                    prs[0].join()
                    prs.remove(prs[0])
                if queue.qsize()>0:
                    print(cnt,end=' ')
                    print(queue.get()[1])
                    cnt=cnt+1
                    if cnt==100:
                        print(time.time()-start)
                        break
            #gethash(f,i)
                #time.ctime(dt)
                
    
    print('start deleting')
    
    try:
        for hs,dtf in df.groupby('hash'):
            if len(dtf)>1:
                kparg=dtf.date.argmin()
                kp=dtf.iloc[dtf.index.get_loc(kparg)]
                sz=kp['size']
                dlt=dtf[~dtf.filename.isin([kp.filename])]
                print('keeping '+os.path.basename(kp.filename)+' ('+time.ctime(kp.date)+') ('+str(sz)+')')
                for f in dlt.filename:
                    d=dtf[dtf.filename.isin([f])]
                    dlsz=d['size'].as_matrix()[0]
                    if abs(kp['size']-dlsz)<100:
                        print('   to delete '+os.path.basename(f)+' ('+time.ctime(d.date)+') ('+str(dlsz)+')',end='')
                        df.loc[df.filename==f,'delete']=True
                        if delete:
                            try:os.remove(f)
                            except:print(' (could not be removed)',end='')
                    else: print(f + ' saved ***********************')
                    print()
    except:
        print('error trying to delete')
    input('finished')
