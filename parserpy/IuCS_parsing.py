# coding: utf-8
'''
Created on  Jan 09, 2018
@author:
    CaoZhen
@description:
    Parsing of IuCS xDR with multiThreading
'''

import os
import threading
import Queue

# Core Parsing Function
# input 1 file once time
def xdrIuCSParsing(fileName, srcPath, targetPath):
    try:
        fout_call = open(os.path.join(targetPath, fileName+'_call.txt'), 'w')
        fout_sms  = open(os.path.join(targetPath, fileName+'_sms.txt'), 'w')
        fout_lau  = open(os.path.join(targetPath, fileName+'_lau.txt'), 'w')
        fout_ho   = open(os.path.join(targetPath, fileName+'_ho.txt'), 'w')
        fout_pag  = open(os.path.join(targetPath, fileName+'_pag.txt'), 'w')
        fout_xx   = open(os.path.join(targetPath, fileName+'_xx.txt'), 'w')

        with open(os.path.join(srcPath, fileName)) as f:
            for line in f:
                l = line.strip().split('|')
                if len(l) == 125:
                    fout_call.write(line)
                elif len(l) == 84:
                    fout_sms.write(line)
                elif len(l) == 77:
                    fout_lau.write(line)
                elif len(l) == 66:
                    fout_ho.write(line)
                elif len(l) == 67:
                    fout_pag.write(line)
                else:
                    fout_xx.write(line)
    
        fout_call.close()    
        fout_sms.close()    
        fout_lau.close()    
        fout_ho.close()    
        fout_pag.close()    
        fout_xx.close()    
        
    except Exception, e:
        print Exception, ':', e

class ThreadPoolMgr():
    def __init__(self, work_queue, thread_num, srcpath, targetpath):
        self.threads = []
        self.work_queue = work_queue
        self.srcpath = srcpath
        self.targetpath = targetpath
        self.init_threadpool(thread_num)

    def init_threadpool(self, thread_num):
        for i in range(thread_num):
            self.threads.append(MyThread(self.work_queue, self.srcpath, self.targetpath))
    
    def wait_allcomplete(self):
        for item in self.threads:
            if item.isAlive():
                item.join()

# MyThread Class, inherited from threading.Thread
class MyThread(threading.Thread):
    def __init__(self, work_queue, srcpath, targetpath):
        threading.Thread.__init__(self)
        self.work_queue = work_queue
        self.srcpath = srcpath
        self.targetpath = targetpath
        self.start()

    def run(self):
        # sel task from query until empty
        while not self.work_queue.empty():
            f = self.work_queue.get()
            # this is the Core Parsing Function
            xdrIuCSParsing(f, self.srcpath, self.targetpath)

if __name__ == '__main__':

    # initialize task queue (file queue)
    fileQueue = Queue.Queue()
    srcPath = '../UNZIPIUCS/'
    for f in os.listdir(srcPath):
        if not os.path.isdir(os.path.join(srcPath, f)):
            fileQueue.put(f)

    # 定义目标路径
    tarPath = '../parseIuCS/'

    # 定义并执行线程池
    p = ThreadPoolMgr(fileQueue, 5, srcPath, tarPath)
    p.wait_allcomplete()
    
    # 主线程
    print 'All files have parsed over.'
