# Transactional consistency

Inconsistencies are common in a data warehouse:

 - In the source system a sales order row and ten line item rows were created and committed. So in the source the order value is always the same as the sum of line item values. As Kafka has two topics for order and line item, the consumer will see orders with missing line items or items that have no matching order record.
 - In the source a booking for account1, -100USD and account2, +100USD was created and committed. Hence the sum of amounts is always zero. The Kafka consumer will get these booking in different partitions and even if they are in the same partition, the poll might get just the first record.
 - An update statement was executed, changing 100'000 rows in the source table.
 - A customer master record was created and committed. 30 seconds later a sales order for this customer created and committed. It might happen that either the consumer gets the records in a different order or it saves the data of the sales order first.

For a classic Data Warehouse loaded just once a day, this can be coped with, e.g. show the data up the the timestamp the first delta load was started.

With Kafka things are more difficult, because neither does Kafka know by default what source records belong to a transaction nor does Kafka provide a global order and because it is a streaming protocol, the consumer does not know if there are more rows of the same transaction are still being produced.

## Add transaction metadata

The first thing the producer must provide is the transaction id in all produced records. This allows the consumer to correlate all source records that belong to the same transaction. What the consumer does not know is if all records of the given transaction id haven been received yet. They might not even have been sent by the producer yet!
This information can be provided in various ways but the most efficient is to add a transaction topic. A topic where the consumer stores the source database commits.

Because each topic partition is handled independently, it is no guarantee that this information is received last, even if produced last. The commit message should also contain information what topics, partitions and min/max offsets the transaction entails. This data is provided by the regular Kafka producer API, so the producer can collect that while sending the records and use it when producing the commit record.

Thanks to the commit topic, its data about where to find the impacted records and the transaction id in the individual records, multiple ways of consuming the data are possible.

## A global consumer

The consumer does listen to all topics including the transaction topic. If all topic/partitions were read up the the max offset of the currently handled transaction, then the data is committed in the target.

This solution preserves the global order, because it is using the transaction topic as the driving information and that topic has a single partition only. It moves the state of the target forward from commit to commit.

But that is also the weak point - it is a single global consumer. All the reasons why Kafka is so fast cannot be utilized.


## A consumer per logical units

In the global consumer no assumptions were made about what a transaction contains. But in reality, a database is modified via APIs, e.g. create-order. There is no API covering the creation of a customer record and create-order. And even if the create-order also does reserve materials, creates a production order and many other things in the same transaction, from an analytical point of view these are rather separate reports.
In other words, it is important to create the order and order lines in a consistent manner, but the other data can be treated independently. If there is no transaction covering more than the order/item pair, it works still perfectly. If a transaction contains more tables, it is broken apart.

This allows for parallel consumption of topics, but each topic must consist of a single partition only to preserve the order.

The main problem with this approach is the global order. The case where a customer master record was created first and the sales order immediately after is not guaranteed - the the consumer of the customer master topic might or might not have written the new record yet.

## RecordNameStrategy - One topic contains different schemas

There is only one option that guarantees the correct order in Kafka - a single partition. To handle the above case, the producer can write all data that belongs together in a single topic/partition. Customer master, sales order row, order line item row,... all goes into the same topic/partition.

There will be the tendency to put all into a single partition, hence ending up with the global consumer again. But this allows to consume data for different areas in parallel. It does not help if all the data of one area is produced faster than a single consumer can handle.

## Partition by

Above it was said, order header and line item must be consistent. But does that mean the sales orders must be consumed in the correct order? What happens when order 10001 is saved first and order 10000 second, although in the source the order was the other way around? The answer is very likely, it has no impact. There are no cases where a single transaction modifies two transactions.

Considering this, a good compromise between performance and consistency can be achieved. For example, if creating a customer master record and a sales order is common, all records can be saved in a single partition. So the partitioning clause is the customer number of customer master and his sales orders for such a case.

What is not possible is to add another dimension, e.g. material master should also be part of the order. As the material has no relationship with the customer id, this is not possible.



## Schema definition

```
{
    "name": "commit",
    "type": "record",
    "fields": [
        {
            "name": "commit_id",
            "type": "string"
        },
        {
            "name": "producer_name",
            "type": "string",
            "default": ""
        },
        {
            "name": "commit_epoch_ns",
            "type": "long"
        },
        {
            "name": "record_count",
            "type": "int",
            "default": 0 
        },
        {
            "name": "topics",
            "type": [
                "null",
                {
                    "type": "map",
                    "values" : {
                        "name": "topic_offsets",
                        "type": "record",
                        "fields": [
                            {
                                "name": "topic_name",
                                "type": "string"
                            },
                            {
                                "name": "schema_names",
                                "type": {
                                    "type": "array",
                                    "items" : "string"
                                }
                            },
                            {
                                "name": "offsets",
                                "type": [
                                    {
                                        "type": "map",
                                        "values": {
                                            "name": "min_max_offsets",
                                            "type": "record",
                                            "fields": [
                                                {
                                                    "name": "min_offset",
                                                    "type": "long"
                                                },
                                                {
                                                    "name": "max_offset",
                                                    "type": "long"
                                                },
                                                {
                                                    "name": "partition",
                                                    "type": "int"
                                                }
                                            ]
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                }
            ]
        }
    ]
}
```