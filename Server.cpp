/*
 * Server.cpp
 *
 *  Created on: 2017年2月17日
 *      Author: wong
 */w

#include <sys/sem.h>

#include "base/Logging.h"
#include "Config.h"
#include "ConfigDefine.h"
#include "LogHelper.h"
#include "SZBusPusher.h"

#define SEM_KEY 4321

int semId = -1;
bool CheckReentry(int argc, char *argv[]) {
  semId = semget(SEM_KEY, 2, 0666 | IPC_CREAT);
  LOG_INFO << "semget " << semId << " " << semctl(semId, 0, GETVAL);
  if (semctl(semId, 0, GETVAL) != 0) {
    if (argc > 1 && strcmp(argv[1], "reload") == 0) {
      LOG_INFO << "reload";
      semctl(semId, 1, SETVAL, 1);
    } else {
      LOG_INFO<< "Reentry. use 'reload' param to reload.";
    }
    return true;
  }
  sembuf sb;
  sb.sem_num = 0;
  sb.sem_op = 1;
  sb.sem_flg = SEM_UNDO;
  semop(semId, &sb, 1);
  LOG_INFO << "Entry.";
  return false;
}

bool WaitReload() {
  sembuf sb;
  sb.sem_num = 1;
  sb.sem_op = -1;
  sb.sem_flg = 0;
  return (semop(semId, &sb, 1) != -1);
}

void Clear() {
  semctl(semId, 0, IPC_RMID);
  semctl(semId, 1, IPC_RMID);
}

int main(int argc, char *argv[])
{
  if (CheckReentry(argc, argv)) {
    return 0;
  }

  CONFIG->Init(CONFIG_PATH);
  LogHelper logHelper(CFGINT(LOG_LEVEL), CFGSTR(LOG_PATH));
  SZBusPusher adapter(CFGSTR(DEVSN), CFGSTR(WHITE_LIST));
  adapter.StartInternalServer(CFGSTR(LISTEN_IP_INT), CFGINT(LISTEN_PORT_INT));
  adapter.StartExternalServer(CFGSTR(LISTEN_IP_EXT), CFGINT(LISTEN_PORT_EXT));
  RdkafkaConsumer consumer(CFGSTR(KAFKA_VERSION), CFGSTR(KAFKA_BROKERS));
  adapter.SetConsumer(&consumer, CFGSTR(KAFKA_TOPIC));

  while (WaitReload()) {
    LOG_INFO << "RefreshWihteList";
    adapter.RefreshWihteList();
  }

  Clear();
  return 0;
}
