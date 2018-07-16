# -*- coding: utf-8 -*-

'''delete duplicate pictures and videos by content
for videos only the first frame is compared
-the oldest file is kept, the newer one is deleted'''

import numpy as np
import cv2
import pandas as pd
import hashlib
import os,time

delete=True
try:
    a=df
except:
    hsh=None
    df=pd.DataFrame()
    #rootdir="e:\\sorted_pictures"
    rootdir=os.path.dirname(os.path.realpath(__file__))
    for root, subdirs, files in os.walk(rootdir):
        print()
        print(root,end='')
        for i,f in enumerate(files):
            lf=f.lower()
            if lf.endswith('.mp4') or lf.endswith('.jpg'):
                if i%100==0 and i>0: print(i,end='')
                print('.',end='')
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
                    df=df.append(pd.DataFrame([[fl,hsh,dt,sz]],columns=['filename','hash','date','size']),ignore_index=True)
                    print(f,hsh)
                #time.ctime(dt)
                

print('starting deleting')

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
