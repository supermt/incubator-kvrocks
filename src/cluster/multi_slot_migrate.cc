//
// Created by supermt on 3/22/23.
//
#include "server/server.h"
#include "slot_migrate.h"

MultiSlotMigrate::MultiSlotMigrate(Server *svr, int migration_speed, int pipeline_size_limit, int seq_gap)
    : SlotMigrate(svr, migration_speed, pipeline_size_limit, seq_gap) {}
Status MultiSlotMigrate::MigrateStart(Server *svr, const std::string &node_id, const std::string &dst_ip, int dst_port,
                                      int slot, int speed, int pipeline_size, int seq_gap) {
  return SlotMigrate::MigrateStart(svr, node_id, dst_ip, dst_port, slot, speed, pipeline_size, seq_gap);
}
