CREATE EXTERNAL TABLE kafka_billings(
        a_party tinyint,
        b_party tinyint,
        delka int,
        cena int,
        zacatek timestamp
                                    )

STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
TBLPROPERTIES(
"kafka.topic" = "billings",
"kafka.bootstrap.servers" = "host:12345"
);