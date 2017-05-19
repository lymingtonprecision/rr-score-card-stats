(ns rrsc.ifs
  (:require [yesql.core :refer [defquery]]))

(defquery rr-shop-order-ids* "rrsc/ifs/queries/rr_shop_order_ids.sql")
(defquery rr-receipts-since* "rrsc/ifs/queries/rr_receipts_since.sql")

(defn rr-shop-order-ids [db]
  (rr-shop-order-ids* {} {:connection db}))

(defn rr-receipts-since [db ts]
  (rr-receipts-since* {:ts ts} {:connection db}))
