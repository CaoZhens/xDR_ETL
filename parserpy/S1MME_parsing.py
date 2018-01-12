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
def xdrS1MMEParsing(fileName, srcPath, targetPath):
    try:
        fout05 = open(os.path.join(targetPath, fileName+'_05.txt'), 'w')
        fout06 = open(os.path.join(targetPath, fileName+'_06.txt'), 'w')
        fout21 = open(os.path.join(targetPath, fileName+'_21.txt'), 'w')
        fout22 = open(os.path.join(targetPath, fileName+'_22.txt'), 'w')
        fout24 = open(os.path.join(targetPath, fileName+'_24.txt'), 'w')
        fout25 = open(os.path.join(targetPath, fileName+'_25.txt'), 'w')
        fout26 = open(os.path.join(targetPath, fileName+'_26.txt'), 'w')
        fout27 = open(os.path.join(targetPath, fileName+'_27.txt'), 'w')
        fout28 = open(os.path.join(targetPath, fileName+'_28.txt'), 'w')
        fout29 = open(os.path.join(targetPath, fileName+'_29.txt'), 'w')
        fout33 = open(os.path.join(targetPath, fileName+'_33.txt'), 'w')
        fout41 = open(os.path.join(targetPath, fileName+'_41.txt'), 'w')
        fout42 = open(os.path.join(targetPath, fileName+'_42.txt'), 'w')
        fout43 = open(os.path.join(targetPath, fileName+'_43.txt'), 'w')
        fout50 = open(os.path.join(targetPath, fileName+'_50.txt'), 'w')
        foutxx = open(os.path.join(targetPath, fileName+'_xx.txt'), 'w')

        with open(os.path.join(srcPath, fileName)) as f:
            for line in f:
                l = line.strip().split('|')
                if len(l) < 3:
                    xdrtype = 'xx'
                else:
                    xdrtype = l[2]

                if xdrtype == '5':
                    fout05.write(line)
                elif xdrtype == '6':
                    fout06.write(line)
                elif xdrtype == '21':
                    fout21.write(line)
                elif xdrtype == '22':
                    fout22.write(line)
                elif xdrtype == '24':
                    fout24.write(line)
                elif xdrtype == '25':
                    fout25.write(line)
                elif xdrtype == '26':
                    fout26.write(line)
                elif xdrtype == '27':
                    fout27.write(line)
                elif xdrtype == '28':
                    fout28.write(line)
                elif xdrtype == '29':
                    fout29.write(line)
                elif xdrtype == '33':
                    fout33.write(line)
                elif xdrtype == '41':
                    fout41.write(line)
                elif xdrtype == '42':
                    fout42.write(line)
                elif xdrtype == '43':
                    fout43.write(line)
                elif xdrtype == '50':
                    fout50.write(line)
                else:
                    foutxx.write(line)
    
        fout05.close()    
        fout06.close()    
        fout21.close()    
        fout22.close()    
        fout24.close()    
        fout25.close()    
        fout26.close()    
        fout27.close()    
        fout28.close()    
        fout29.close()    
        fout33.close()    
        fout41.close()    
        fout42.close()    
        fout43.close()    
        fout50.close()    
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
