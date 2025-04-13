package nettee.persistence.id.snowflake;

import nettee.persistence.id.snowflake.exception.ClockBackwardException;

import static nettee.persistence.id.snowflake.SnowflakeConstants.SnowflakeDefault.DATACENTER_ID_SHIFT;
import static nettee.persistence.id.snowflake.SnowflakeConstants.SnowflakeDefault.SEQUENCE_MASK;
import static nettee.persistence.id.snowflake.SnowflakeConstants.SnowflakeDefault.TIMESTAMP_LEFT_SHIFT;
import static nettee.persistence.id.snowflake.SnowflakeConstants.SnowflakeDefault.WORKER_ID_SHIFT;
import static nettee.persistence.id.snowflake.validator.SnowflakeConstructingValidator.validateDatacenterId;
import static nettee.persistence.id.snowflake.validator.SnowflakeConstructingValidator.validateWorkerId;

public final class Snowflake {
    private final long datacenterId;
    private final long workerId;
    private final long epoch;

    private long sequence;
    private long lastTimestamp = -1L;

    public Snowflake(SnowflakeProperties properties) {
        this(
                properties.datacenterId(),
                properties.workerId(),
                properties.epoch()
        );
    }

    public Snowflake(long datacenterId, long workerId, long epoch) {
        validateDatacenterId(datacenterId);
        validateWorkerId(workerId);

        this.datacenterId = datacenterId;
        this.workerId = workerId;
        this.epoch = epoch;
    }

    // synchronized: 동시성 제어를 엄격하게 할 때 사용
    public synchronized long nextId() {
        var timestamp = timeGen();

        if (timestamp < lastTimestamp) { // clock backward
            throw new ClockBackwardException(timestamp, lastTimestamp);
        }

        if (timestamp == lastTimestamp) {
            // 원래 값(오버플로): 0000 ... 0001 0000 0000 0000
            // 마스크:          0000 ... 0000 1111 1111 1111
            // 결괏값:          0000 ... 0000 0000 0000 0000
            sequence = (sequence + 1) & SEQUENCE_MASK;
            if (sequence == 0) { // overflow
                // 다음 밀리초 기다리기
                timestamp = tilNextMillis(lastTimestamp);
            }
        } else { // timestamp > lastTimestamp
            sequence = 0L;
        }

        lastTimestamp = timestamp;

        long id;

        id = (timestamp - epoch) << TIMESTAMP_LEFT_SHIFT;
        id |= datacenterId << DATACENTER_ID_SHIFT;
        id |= workerId << WORKER_ID_SHIFT;
        id |= sequence;

        return id;
//        return ((timestamp - epoch) << TIMESTAMP_LEFT_SHIFT) |
//                (datacenterId << DATACENTER_ID_SHIFT) |
//                (workerId << WORKER_ID_SHIFT) |
//                sequence;
    }

    private long tilNextMillis(long lastTimestamp) {
        // busy-wait: 가상스레드 환경에서 특히 안 좋지만, 이 로직에서는 다음 이유로 사용합니다.
        //  (1) 실제 대기 시간이 굉장히 드물고 짧은 순간 발생
        //  (2) 점유·해제를 오가는 동안 Context switching 비용을 고려
        //  (3) 점유를 회피하여 개선을 기대하는 것보다(개선될지 아닐지도 모르지만)
        //      다음 밀리초 때 빠르게 아이디를 제공하는 게 오히려
        //      신속한 아이디 제공으로 병목 시간 감소할 가능성 높음.
        //
        //  이상 이유로:
        //    LockSupport.parkNanos(), Thread.sleep() 등을 사용하는 것보다
        //    busy wait로 짧은 시간 CPU를 점유하는 게 나을 수 있습니다.
        long timestamp = timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = timeGen();
        }
        return timestamp;
    }

    private long timeGen() {
        return System.currentTimeMillis();
    }
}