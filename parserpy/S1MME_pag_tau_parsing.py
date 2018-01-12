# coding: utf-8
'''
Created on  Jan 10, 2018
@author:
    CaoZhen
@description:
    Parsing of S1MME xDR - ONLY PAG & TAU with multiThreading
'''

import os
import threading
import Queue

# Core Parsing Function
# input 1 file once time
def xdrS1MMEParsing(fileName, srcPath, targetPath):
    try:
        fout24 = open(os.path.join(targetPath, fileName+'_24.txt'), 'w')
        fout43 = open(os.path.join(targetPath, fileName+'_43.txt'), 'w')

        with open(os.path.join(srcPath, fileName)) as f:
            for line in f:
                l = line.strip().split('|')
                if len(l) < 3:
                    xdrtype = 'xx'
                else:
                    xdrtype = l[2]

                if xdrtype == '24':
                    fout24.write(line)
                if xdrtype == '43':
                    fout43.write(line)
        
        fout24.close()        
        fout43.close()
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
            xdrS1MMEParsing(f, self.srcpath, self.targetpath)

if __name__ == '__main__':

    # initialize task queue (file queue)
    fileQueue = Queue.Queue()
    srcPath = '../UNZIPS1MME/'
    for f in os.listdir(srcPath):
        if not os.path.isdir(os.path.join(srcPath, f)):
            fileQueue.put(f)

    # 定义目标路径
    tarPath = '../parseS1MME/'

    # 定义并执行线程池
    p = ThreadPoolMgr(fileQueue, 5, srcPath, tarPath)
    p.wait_allcomplete()
    
    # 主线程
    print 'All files have parsed over.'