#pragma once


struct Message {
    uint32_t message_id;
    uint8_t source_id;
    uint8_t destination_id;
    uint8_t origin_id;
    bool ack;
};
