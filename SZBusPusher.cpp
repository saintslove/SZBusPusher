/*
 * SZBusPusher.cpp
 *
 *  Created on: 2017年2月17日
 *      Author: wong
 */

#include "SZBusPusher.h"

#include <cstring>
#include <stdio.h>
#include <stdlib.h>
#include <boost/bind.hpp>

#include "base/Logging.h"

#include "BATNetSDKRawAPI.h"
#include "BATNetSDKAPI.h"
#include "SZBusProtocolAPI.h"
#include "Config.h"


#define CHECK_RET(exp) \
    { \
        int ret = exp; \
        if (ret != CCMS_RETURN_OK) \
        { \
            LOG_WARN << #exp << " = " << ret; \
            break; \
        } \
    }

SZBusPusher::SZBusPusher(const std::string& sn, const std::string& whiteListFile)
: m_hIntServer(-1)
, m_hExtServer(-1)
, m_threadCheck(
    boost::bind(&SZBusPusher::CheckThreadFunc, this), "CheckThread")
, m_threadPush(
    boost::bind(&SZBusPusher::PushThreadFunc, this), "PushThread")
, m_bCheckThreadRunning(true)
, m_bPushThreadRunning(true)
, m_whiteListFile(whiteListFile)
, m_mutexConn()
, m_condConn(m_mutexConn)
, m_mutexMsg()
, m_condMsg(m_mutexMsg)
{
    BATNetSDK_Init(CCMS_DEVTYPE_DELIVER, (char*)sn.c_str(), true);
    m_threadCheck.start();
    m_threadPush.start();
}

SZBusPusher::~SZBusPusher()
{
    m_bCheckThreadRunning = false;
    m_bPushThreadRunning = false;
    m_threadCheck.join();
    m_threadPush.join();
    if (m_hIntServer != -1)
    {
        BATNetSDK_DeleteObj(m_hIntServer);
    }
    if (m_hExtServer != -1)
    {
        BATNetSDKRaw_DeleteObj (m_hExtServer);
    }
    BATNetSDK_Release();
}

void SZBusPusher::CheckThreadFunc()
{
    while (m_bCheckThreadRunning)
    {
        muduo::MutexLockGuard lock(m_mutexConn);
        LoadWhiteList();
        CheckClientList();
        m_condConn.waitForSeconds(60 * 5); // 5分钟
    }
}

void SZBusPusher::PushThreadFunc()
{
    while (m_bPushThreadRunning)
    {
        std::vector<SendData> sendQueue;
        {
            muduo::MutexLockGuard lock(m_mutexMsg);
            if (m_sendQueue.empty())
            {
                m_condMsg.waitForMillSeconds(10);
                continue;
            }
            else
            {
                LOG_DEBUG << "m_packgeQueue.size() = " << m_sendQueue.size();
                sendQueue.swap(m_sendQueue);
            }
        }
        if (sendQueue.size() > 1000)
        {
            LOG_WARN<< " sendQueue too long! " << sendQueue.size();
        }
        for (size_t i = 0; i < sendQueue.size(); ++i)
        {
            if (sendQueue[i].buf == NULL)
            {
                LOG_ERROR<< i << " buf == NULL len = " << sendQueue[i].len;
                continue;
            }
            DoPusher(sendQueue[i].buf, sendQueue[i].len);
            SAFE_DELETEA(sendQueue[i].buf);
        }
        sendQueue.clear();
    }
}

void SZBusPusher::SetConsumer(RdkafkaConsumer* consumer, const std::string& topic)
{
    m_consumer = consumer;
    m_consumer->Consume(topic, ConsumeCB, this);
}

int SZBusPusher::StartInternalServer(const std::string& ip, uint16_t port)
{
    LOG_INFO << "StartInternalServer " << ip << " " << port;
    CCMS_NETADDR addr = { {0}, 0};
    memcpy(addr.chIP, ip.c_str(), ip.length());
    addr.nPort = port;
    m_hIntServer = BATNetSDK_CreateServerObj(&addr);
    BATNetSDK_SetMsgCallBack(m_hIntServer, IntRecvCB, this);
    BATNetSDK_Start(m_hIntServer);
    return 0;
}

int SZBusPusher::StartExternalServer(const std::string& ip, uint16_t port)
{
    LOG_INFO << "StartExternalServer " << ip << " " << port;
    CCMS_NETADDR addr = { {0}, 0};
    memcpy(addr.chIP, ip.c_str(), ip.length());
    addr.nPort = port;
    m_hExtServer = BATNetSDKRaw_CreateServerObj(&addr);
    BATNetSDKRaw_SetConnCallBack(m_hExtServer, ExtConnCB, this);
    //BATNetSDKRaw_SetMsgCallBack(m_hIntServer, RecvCB, this);
    BATNetSDKRaw_Start(m_hExtServer);
    return 0;
}

int SZBusPusher::RefreshWihteList()
{
    muduo::MutexLockGuard lock(m_mutexConn);
    m_condConn.notifyAll();
    return 0;
}

void SZBusPusher::OnConsume(const std::string& key, void* payload, size_t len) {
  LOG_DEBUG << key << " " << (char*) payload;
  char* sendBuf = NULL;
  do {
    static int offset = SZBus_GetBodyOffset();
    int packLen = 0;
    int bodyLen = 0;
    SZBusHeader header;
    if (key == "PositionInfo") {
      PositionInfo info;
      CHECK_RET(SZBus_Str2PositionInfo((char* ) payload, len, &info));
      packLen = SZBus_GetPackLen(SZBusMsg_PositionInfo);
      bodyLen = SZBus_GetBodyLen(SZBusMsg_PositionInfo);
      sendBuf = new char[packLen];
      CHECK_RET(SZBus_PackPositionInfo(&info, sendBuf + offset, &bodyLen));
      header.u1MsgId = SZBusMsg_PositionInfo;
    } else if (key == "ArriveStop") {
      ArriveStop info;
      CHECK_RET(SZBus_Str2ArriveStop((char* ) payload, len, &info));
      packLen = SZBus_GetPackLen(SZBusMsg_ArriveStop);
      bodyLen = SZBus_GetBodyLen(SZBusMsg_ArriveStop);
      sendBuf = new char[packLen];
      CHECK_RET(SZBus_PackArriveStop(&info, sendBuf + offset, &bodyLen));
      header.u1MsgId = SZBusMsg_ArriveStop;
    } else if (key == "LeaveStop") {
      LeaveStop info;
      CHECK_RET(SZBus_Str2LeaveStop((char* ) payload, len, &info));
      packLen = SZBus_GetPackLen(SZBusMsg_LeaveStop);
      bodyLen = SZBus_GetBodyLen(SZBusMsg_LeaveStop);
      sendBuf = new char[packLen];
      CHECK_RET(SZBus_PackLeaveStop(&info, sendBuf + offset, &bodyLen));
      header.u1MsgId = SZBusMsg_LeaveStop;
    } else if (key == "ArriveStation") {
      ArriveStation info;
      CHECK_RET(SZBus_Str2ArriveStation((char* ) payload, len, &info));
      packLen = SZBus_GetPackLen(SZBusMsg_ArriveStation);
      bodyLen = SZBus_GetBodyLen(SZBusMsg_ArriveStation);
      sendBuf = new char[packLen];
      CHECK_RET(SZBus_PackArriveStation(&info, sendBuf + offset, &bodyLen));
      header.u1MsgId = SZBusMsg_ArriveStation;
    } else if (key == "LeaveStation") {
      LeaveStation info;
      CHECK_RET(SZBus_Str2LeaveStation((char* ) payload, len, &info));
      packLen = SZBus_GetPackLen(SZBusMsg_LeaveStation);
      bodyLen = SZBus_GetBodyLen(SZBusMsg_LeaveStation);
      sendBuf = new char[packLen];
      CHECK_RET(SZBus_PackLeaveStation(&info, sendBuf + offset, &bodyLen));
      header.u1MsgId = SZBusMsg_LeaveStation;
    } else if (key == "AlarmInfo") {
      AlarmInfo info;
      CHECK_RET(SZBus_Str2AlarmInfo((char* ) payload, len, &info));
      packLen = SZBus_GetPackLen(SZBusMsg_AlarmInfo);
      bodyLen = SZBus_GetBodyLen(SZBusMsg_AlarmInfo);
      sendBuf = new char[packLen];
      CHECK_RET(SZBus_PackAlarmInfo(&info, sendBuf + offset, &bodyLen));
      header.u1MsgId = SZBusMsg_AlarmInfo;
    } else {
      LOG_WARN<< "unknown key: " << key;
      break;
    }
    static uint8_t sq = 0;
    header.u1Sequence = sq++;
    header.u1RecvRole = RR_Platform;
    header.u1SendRole = SR_None;
    header.u4RecvAddr = 0;
    header.u4SendAddr = 0;
    CHECK_RET(SZBus_PackPackage(header, sendBuf + offset, bodyLen, sendBuf, &packLen));
    DoPusher(sendBuf, packLen);
  } while (false);
  SAFE_DELETEA(sendBuf);
}

int SZBusPusher::OnIntRecv(int sessionId, int msgId, const char* buf, int len)
{
    //LOG_INFO << "buf : " << buf;
//    printf("len=%d, ", len);
//    for (int i = 0; i < len; ++i)
//    {
//        printf("%02x ", (unsigned char)*(buf + i));
//    }
//    printf("\n");

    if (msgId != CCMS_SZBUSGPS_MSG)
    {
        LOG_WARN<< "wrong package. msgid=" << msgId << " len=" << len;
        return -1;
    }

    SZBusHeader header;
    const char* pBody = NULL;
    int parseLen = len, bodyLen = 0;
    int ret = SZBus_ParsePackage(buf, &parseLen, &header, &pBody, &bodyLen);
    LOG_INFO << "ret = " << ret << " parseLen = " << parseLen
        << " header: " << header.u1MsgId << " " << header.u2Length;
    LOG_DEBUG << "ret = " << ret << " parseLen = " << parseLen
        << " header: " << header.u1MsgId << " " << header.u2Length
        << " " << header.u1Sequence << " " << header.u1RecvRole << " " << header.u1SendRole
        << " " << header.u4RecvAddr << " " << header.u4SendAddr;
    if (ret == CCMS_RETURN_OK)
    {
//        int packLen = SZBus_GetPackLen((SZBusMsg)header.u1MsgId);
//        if (packLen <= 0)
//        {
//            LOG_WARN << "Unknown msg " << header.u1MsgId << " len = " << header.u2Length;
//            return parseLen;
//        }
        int bodyLen = SZBus_GetBodyLen((SZBusMsg)header.u1MsgId);
        if (bodyLen <= 0)
        {
            LOG_WARN << "Unknown msg " << header.u1MsgId << " len = " << header.u2Length;
            return parseLen;
        }
//        int offset = SZBus_GetBodyOffset();
//        char* sendBuf = new char[packLen];
        bool bSuccess = false;
        switch (header.u1MsgId)
        {
        case SZBusMsg_PositionInfo:
            {
                PositionInfo positionInfo;
                CHECK_RET(SZBus_ParsePositionInfo(pBody, &bodyLen, &positionInfo));
//                CHECK_RET(SZBus_PackPositionInfo(&positionInfo, sendBuf + offset, &bodyLen));
//                CHECK_RET(SZBus_PackPackage(header, sendBuf + offset, bodyLen, sendBuf, &packLen));
                bSuccess = true;
            }
            break;
        case SZBusMsg_ArriveStop:
            {
                ArriveStop arriveStop;
                CHECK_RET(SZBus_ParseArriveStop(pBody, &bodyLen, &arriveStop));
//                CHECK_RET(SZBus_PackArriveStop(&arriveStop, sendBuf + offset, &bodyLen));
//                CHECK_RET(SZBus_PackPackage(header, sendBuf + offset, bodyLen, sendBuf, &packLen));
                bSuccess = true;
            }
            break;
        case SZBusMsg_LeaveStop:
            {
                LeaveStop leaveStop;
                CHECK_RET(SZBus_ParseLeaveStop(pBody, &bodyLen, &leaveStop));
//                CHECK_RET(SZBus_PackLeaveStop(&leaveStop, sendBuf + offset, &bodyLen));
//                CHECK_RET(SZBus_PackPackage(header, sendBuf + offset, bodyLen, sendBuf, &packLen));
                bSuccess = true;
            }
            break;
        case SZBusMsg_ArriveStation:
            {
                ArriveStation arriveStation;
                CHECK_RET(SZBus_ParseArriveStation(pBody, &bodyLen, &arriveStation));
//                CHECK_RET(SZBus_PackArriveStation(&arriveStation, sendBuf + offset, &bodyLen));
//                CHECK_RET(SZBus_PackPackage(header, sendBuf + offset, bodyLen, sendBuf, &packLen));
                bSuccess = true;
            }
            break;
        case SZBusMsg_LeaveStation:
            {
                LeaveStation leaveStation;
                CHECK_RET(SZBus_ParseLeaveStation(pBody, &bodyLen, &leaveStation));
//                CHECK_RET(SZBus_PackLeaveStation(&leaveStation, sendBuf + offset, &bodyLen));
//                CHECK_RET(SZBus_PackPackage(header, sendBuf + offset, bodyLen, sendBuf, &packLen));
                bSuccess = true;
            }
            break;
        case SZBusMsg_AlarmInfo:
            {
                AlarmInfo alarmInfo;
                CHECK_RET(SZBus_ParseAlarmInfo(pBody, &bodyLen, &alarmInfo));
//                CHECK_RET(SZBus_PackAlarmInfo(&alarmInfo, sendBuf + offset, &bodyLen));
//                CHECK_RET(SZBus_PackPackage(header, sendBuf + offset, bodyLen, sendBuf, &packLen));
                bSuccess = true;
            }
            break;
        default:
            LOG_ERROR << "error msg " << header.u1MsgId;
            assert(false);
            break;
        }
        if (bSuccess)
        {
            int packLen = SZBus_GetPackLen((SZBusMsg)header.u1MsgId);
            char* sendBuf = new char[packLen];
            memcpy(sendBuf, buf, packLen);
            muduo::MutexLockGuard lock(m_mutexMsg);
            SendData sendData = {sendBuf, (size_t)packLen};
            m_sendQueue.push_back(sendData);
        }
//        else
//        {
//            SAFE_DELETEA(sendBuf);
//        }
        return parseLen;
    }
    else if (ret == CCMS_RETURN_CONTINUE)
    {
        return 0;
    }
    else
    {
        LOG_WARN << "ret = " << ret << " len = " << len << " parseLen = " << parseLen;
        return -1;
    }
}

int SZBusPusher::OnExtConn(int sessionId, int status, const char* ip, unsigned short port)
{
    LOG_INFO << "OnExtConn " << sessionId << " " << status << " " << ip << ":" << port;
    std::string strIP = ip;
    if (status == 0)
    {
        {
            muduo::MutexLockGuard lock(m_mutexConn);
            if (std::count(m_whiteList.begin(), m_whiteList.end(), strIP) == 0)
            {
                LOG_WARN << "not in whitelist! " << strIP;
                return -1;
            }
        }
        if (m_clientList.count(strIP) >= 3)
        {
            LOG_WARN << "reach the conn limit of ip " << strIP;
            return -1;
        }
        for (ClientMap::iterator it = m_clientList.lower_bound(strIP);
            it != m_clientList.upper_bound(strIP); ++it)
        {
            if (it->second.first == port)
            {
                return 0; // wbf.mark
            }
        }
        m_clientList.insert(std::make_pair(strIP, std::make_pair(port, sessionId)));
    }
    else if (status == 1)
    {
        for (ClientMap::iterator it = m_clientList.lower_bound(strIP);
            it != m_clientList.upper_bound(strIP); ++it)
        {
            if (it->second.first == port)
            {
                m_clientList.erase(it);
                break;
            }
        }
    }
    return 0;
}

void SZBusPusher::DoPusher(const char* buf, size_t len)
{
    BATNetSDKRaw_SendAll(m_hExtServer, buf, len);
}

void SZBusPusher::LoadWhiteList()
{
    m_whiteList.clear();

    FILE * fp;
    fp = fopen(m_whiteListFile.c_str(), "r");
    if (fp == NULL)
    {
        LOG_ERROR<< "WhiteList fopen failed! path = " << m_whiteListFile;
        return;
    }

    LOG_INFO << "load WhiteList:";
    char line[1024] = {0};
    char ip[64] = {0};
    while (fgets(line, 1024, fp))
    {
        if (line[0] == '#' || strcmp(line, "") == 0)
        {
            continue;
        }
        ip[0] = 0;
        sscanf(line, "%s", ip);
        if (strcmp(ip, "") == 0)
        {
            continue;
        }
        m_whiteList.push_back(ip);
        LOG_INFO << "  > " << ip;
    }

    fclose(fp);
}

void SZBusPusher::CheckClientList()
{
    for (ClientMap::iterator it = m_clientList.begin(); it != m_clientList.end();)
    {
        std::string ip = it->first;
        if (std::count(m_whiteList.begin(), m_whiteList.end(), ip) == 0)
        {
            for (; it != m_clientList.upper_bound(ip); ++it)
            {
                BATNetSDKRaw_Disconnect(m_hExtServer, it->second.second);
            }
            size_t count = m_clientList.erase(ip);
            LOG_INFO << "Disconnect [" << count << "] clients not in whiteList";
        }
        it = m_clientList.upper_bound(ip);
    }
}
