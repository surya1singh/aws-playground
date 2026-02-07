Problem: Build an enterprise-grade batch analytics warehouse from snapshot-based operational extracts.

Input: Daily full snapshots (no CDC flags) land in S3 for 12 operational tables:
customers, customer_addresses, customer_accounts, products, product_categories, suppliers, inventory, orders, order_items, payments, shipments, returns.

Requirements:
1. Load day-1 snapshot as a full load
2. For day-2+, process incrementally by comparing snapshots:
   * infer inserts/updates/deletes
3. Build Postgres star schema:
   * dim_customer + dim_customer_address + dim_product as SCD2
   * facts for orders, order_items, payments, shipments, returns
4. Handle:
   * late arriving dimensions (unknown keys + backfill)
   * soft deletes (missing keys N days)
   * idempotent re-runs (same dt safe)
5. Provide:
   * Mapping Spec + HLD + LLD
   * Glue job code + tests + CI pipeline


--
Raw source tables:
* customers
* customer_addresses
* customer_accounts
* products
* product_categories
* suppliers
* inventory
* orders
* order_items
* payments
* shipments
* returns