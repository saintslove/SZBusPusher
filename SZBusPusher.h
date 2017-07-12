/*
 * SZBusPusher.h
 *
 *  Created on: 2017年2月17日
 *      Author: wong
 */

#ifndef SZBUSPUSHER_H_
#define SZBUSPUSHER_H_

#include <stdint.h>
#include <string>
#include <vector>
//#include <queue>
#include <map>

#include "base/Thread.h"
#include "base/Mutex.h"
#include "base/Condition.h"

#include "RdkafkaConsumer.h"

class SZBusPusher
{
public:
    SZBusPusher(const std::string& sn, const std::string& whiteListFile);
    virtual ~SZBusPusher();

public:
    void SetConsumer(RdkafkaConsumer* consumer, const std::string& topic);
    int StartInternalServer(const std::string& ip, uint16_t port);
    int StartExternalServer(const std::string& ip, uint16_t port);
    int RefreshWihteList();

private:
    static void ConsumeCB(const std::string& key, void* payload, size_t len, void* userdata)
    {
        SZBusPusher* that = reinterpret_cast<SZBusPusher*>(userdata);
        return that->OnConsume(key, payload, len);
    }
    void OnConsume(const std::string& key, void* payload, size_t len);
    static int IntRecvCB(int sessionId, int msgId, const char* buf, int len, void* userdata)
    {
        SZBusPusher* that = reinterpret_cast<SZBusPusher*>(userdata);
        return that->OnIntRecv(sessionId, msgId, buf, len);
    }
    int OnIntRecv(int sessionId, int msgId, const char* buf, int len);
    static int ExtConnCB(int sessionId, int status, const char* ip, unsigned short port, void* userdata)
    {
        SZBusPusher* that = reinterpret_cast<SZBusPusher*>(userdata);
        return that->OnExtConn(sessionId, status, ip, port);
    }
    int OnExtConn(int sessionId, int status, const char* ip, unsigned short port);

private:
    void CheckThreadFunc(); // push thread
    void PushThreadFunc();  // check thread

private:
    void DoPusher(const char* buf, size_t len);
    void LoadWhiteList();
    void CheckClientList();

private:
    int m_hIntServer;
    int m_hExtServer;
    muduo::Thread m_threadCheck;
    muduo::Thread m_threadPush;
    bool m_bCheckThreadRunning;
    bool m_bPushThreadRunning;
    std::string m_whiteListFile;
    std::vector<std::string> m_whiteList;

    muduo::MutexLock m_mutexConn;
    muduo::Condition m_condConn;
    typedef std::multimap<std::string, std::pair<unsigned short, int> > ClientMap;
    ClientMap m_clientList;

    struct SendData
    {
        char* buf;
        size_t len;
    };
    muduo::MutexLock m_mutexMsg;
    muduo::Condition m_condMsg;
    std::vector<SendData> m_sendQueue;

    RdkafkaConsumer* m_consumer;
};

#endif /* SZBUSPUSHER_H_ */
