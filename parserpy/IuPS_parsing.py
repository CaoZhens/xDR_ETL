# coding: utf-8
'''
Created on  Jan 10, 2018
@author:
    CaoZhen
@description:
    Parsing of IuPS xDR with multiThreading
'''

import os
import threading
import Queue

# Core Parsing Function
# input 1 file once time
def xdrIuPSParsing(fileName, srcPath, targetPath):
    try:
        fout51 = open(os.path.join(targetPath, fileName+'_51.txt'), 'w')
        fout52 = open(os.path.join(targetPath, fileName+'_52.txt'), 'w')
        fout53 = open(os.path.join(targetPath, fileName+'_53.txt'), 'w')
        fout54 = open(os.path.join(targetPath, fileName+'_54.txt'), 'w')
        fout55 = open(os.path.join(targetPath, fileName+'_55.txt'), 'w')
        fout56 = open(os.path.join(targetPath, fileName+'_56.txt'), 'w')
        fout57 = open(os.path.join(targetPath, fileName+'_57.txt'), 'w')
        fout58 = open(os.path.join(targetPath, fileName+'_58.txt'), 'w')
        fout5 =  open(os.path.join(targetPath, fileName+'_5.txt'), 'w')
        foutxx = open(os.path.join(targetPath, fileName+'_xx.txt'), 'w')

        with open(os.path.join(srcPath, fileName)) as f:
            for line in f:
                l = line.strip().split('|')
                if len(l) < 3:
                    xdrtype = 'xx'
                else:
                    xdrtype = l[2]

                if xdrtype == '51':
                    fout51.write(line)
                elif xdrtype == '52':
                    fout52.write(line)
                elif xdrtype == '53':
                    fout53.write(line)
                elif xdrtype == '54':
                    fout54.write(line)
                elif xdrtype == '55':
                    fout55.write(line)
                elif xdrtype == '56':
                    fout56.write(line)
                elif xdrtype == '57':
                    fout57.write(line)
                elif xdrtype == '58':
                    fout58.write(line)
                elif xdrtype == '5':
                    fout5.write(line)
                else:
                    foutxx.write(line)
    
        fout51.close()
        fout52.close()    
        fout53.close()    
        fout54.close()    
        fout55.close()    
        fout56.close()    
        fout57.close()    
        fout58.close()    
        fout5.close()    
        foutxx.close()
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
            xdrIuPSParsing(f, self.srcpath, self.targetpath)

if __name__ == '__main__':

    # initialize task queue (file queue)
    fileQueue = Queue.Queue()
    srcPath = '../UNZIPIUPS/'
    for f in os.listdir(srcPath):
        if not os.path.isdir(os.path.join(srcPath, f)):
            fileQueue.put(f)

    # 定义目标路径
    tarPath = '../parseIuPS/'

    # 定义并执行线程池
    p = ThreadPoolMgr(fileQueue, 5, srcPath, tarPath)
    p.wait_allcomplete()
    
    # 主线程
    print 'All files have parsed over.'