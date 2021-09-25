### Checkin
SELECT
    "replica_full"."person_transactions"."person_id" AS "person_id",
    COUNT("replica_full"."person_transactions"."person_id") AS "count checkin"
FROM 
    "replica_full"."person_transactions"
WHERE (
    ("replica_full"."person_transactions"."considered_at_converted" >= timestamp '2021-01-12 00:00 UTC' AND "replica_full"."person_transactions"."considered_at_converted" <= timestamp '2021-02-28 00:00 UTC')
    OR ("replica_full"."person_transactions"."considered_at_converted" >= timestamp '2021-04-12 00:00 UTC' AND "replica_full"."person_transactions"."considered_at_converted" <= timestamp '2021-05-31 00:00 UTC')
   AND "replica_full"."person_transactions"."country_id" = 76
   )
GROUP BY "replica_full"."person_transactions"."person_id"
ORDER BY "count checkin" DESC


### Users Plan
SELECT "replica_full"."person_products"."person_id" AS "person_id", max("replica_full"."person_products"."total_price") AS "max"
FROM "replica_full"."person_products"
WHERE (
    ("replica_full"."person_products"."paid_at" >= timestamp '2021-01-12 00:00 UTC' AND "replica_full"."person_products"."paid_at" <= timestamp '2021-02-28 00:00 UTC')
    OR ("replica_full"."person_products"."paid_at" >= timestamp '2021-04-12 00:00 UTC' AND "replica_full"."person_products"."paid_at" <= timestamp '2021-05-31 00:00 UTC')
   AND "replica_full"."person_products"."country_id" = 76
   )
GROUP BY "replica_full"."person_products"."person_id"
ORDER BY "replica_full"."person_products"."person_id" ASC


### Cancelamentos
SELECT 
    "replica_full"."person_product_changes"."person_id" AS "person_id", 
    "replica_full"."person_product_changes"."change_to_type" AS "change_to_type", 
    date_trunc('day', "replica_full"."person_product_changes"."requested_at") AS "requested_at", 
    date_trunc('day', "replica_full"."person_product_changes"."scheduled_at") AS "scheduled_at", 
    "replica_full"."person_product_changes"."reason" AS "reason", 
    "replica_full"."person_product_changes"."other_reason" AS "other_reason", 
    "replica_full"."person_product_changes"."additional_comment" AS "additional_comment", 
count(*) AS "count"
FROM "replica_full"."person_product_changes"
LEFT JOIN "replica_full"."person_products" "person_products" ON "replica_full"."person_product_changes"."person_id" = "person_products"."person_id"
WHERE 
    (
    ("replica_full"."person_product_changes"."requested_at" >= timestamp '2021-04-12 00:00 UTC' AND "replica_full"."person_product_changes"."requested_at" <= timestamp '2021-05-31 00:00 UTC') 
    AND ("replica_full"."person_product_changes"."change_to_type" = 0 OR "replica_full"."person_product_changes"."change_to_type" = 20) 
    AND "person_products"."country_id" = 76
    )
GROUP BY "replica_full"."person_product_changes"."person_id", "replica_full"."person_product_changes"."change_to_type", date_trunc('day', "replica_full"."person_product_changes"."requested_at"), date_trunc('day', "replica_full"."person_product_changes"."scheduled_at"), "replica_full"."person_product_changes"."reason", "replica_full"."person_product_changes"."other_reason", "replica_full"."person_product_changes"."additional_comment"